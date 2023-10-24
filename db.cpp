/************************************************************
	Project#1:	CLP & DDL
 ************************************************************/


#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <search.h>
#include <ctype.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/types.h>

/********************************************************************
db.h - This file contains all the structures, defines, and function
	prototype for the db.exe program.
*********************************************************************/

#define MAX_IDENT_LEN   16
#define MAX_NUM_COL			16
#define MAX_TOK_LEN			32
#define KEYWORD_OFFSET	10
#define STRING_BREAK		" (),<>="
#define NUMBER_BREAK		" ),"
#define MAX_STRING_LEN 255
#define MAX_NUM_ROW 100
#define MAX_NUM_TABLE 100
#define MAX_NUM_CONDITION 2
#define ROLLFORWARD_PENDING 1
#define LOG_ENTRY_TIMESTAMP_LEN 14
#define MAX_LOG_ENTRY_TEXT_LEN 1000
#define MAX_NUM_LOG_BACKUP_FILES 999

/* Column descriptor sturcture = 20+4+4+4+4 = 36 bytes */
typedef struct cd_entry_def
{
	char		col_name[MAX_IDENT_LEN+4];
	int			col_id;                   /* Start from 0 */
	int			col_type;
	int			col_len;
	int 		not_null;
} cd_entry;

/* Table packed descriptor sturcture = 4+20+4+4+4 = 36 bytes
   Minimum of 1 column in a table - therefore minimum size of
	 1 valid tpd_entry is 36+36 = 72 bytes. */
typedef struct tpd_entry_def
{
	int				tpd_size;
	char			table_name[MAX_IDENT_LEN+4];
	int				num_columns;
	int				cd_offset;
	int       tpd_flags;
} tpd_entry;

/* Table packed descriptor list = 4+4+4+36 = 48 bytes.  When no
   table is defined the tpd_list is 48 bytes.  When there is 
	 at least 1 table, then the tpd_entry (36 bytes) will be
	 overlapped by the first valid tpd_entry. */
typedef struct tpd_list_def
{
	int				list_size;
	int				num_tables;
	int				db_flags;
	tpd_entry	tpd_start;
}tpd_list;

/* This token_list definition is used for breaking the command
   string into separate tokens in function get_tokens().  For
	 each token, a new token_list will be allocated and linked 
	 together. */
typedef struct t_list
{
	char	tok_string[MAX_TOK_LEN];
	int		tok_class;
	int		tok_value;
	struct t_list *next;
} token_list;

/* This enum defines the different classes of tokens for 
	 semantic processing. */
typedef enum t_class
{
	keyword = 1,	// 1
	identifier,		// 2
	symbol, 			// 3
	type_name,		// 4
	constant,		  // 5
  function_name,// 6
	terminator,		// 7
	error			    // 8
  
} token_class;

/* This enum defines the different values associated with
   a single valid token.  Use for semantic processing. */
typedef enum t_value
{
	T_INT = 10,		// 10 - new type should be added above this line
	T_VARCHAR,		    // 11 
	T_CHAR,		    // 12       
	K_CREATE, 		// 13
	K_TABLE,			// 14
	K_NOT,				// 15
	K_NULL,				// 16
	K_DROP,				// 17
	K_LIST,				// 18
	K_SCHEMA,			// 19
  K_FOR,        // 20
	K_TO,				  // 21
  K_INSERT,     // 22
  K_INTO,       // 23
  K_VALUES,     // 24
  K_DELETE,     // 25
  K_FROM,       // 26
  K_WHERE,      // 27
  K_UPDATE,     // 28
  K_SET,        // 29
  K_SELECT,     // 30
  K_ORDER,      // 31
  K_BY,         // 32
  K_DESC,       // 33
  K_IS,         // 34
  K_AND,        // 35
  K_OR,         // 36 - new keyword should be added below this line
  F_SUM,        // 37
  F_AVG,        // 38
	F_COUNT,      // 39 - new function name should be added below this line
	S_LEFT_PAREN = 70,  // 70
	S_RIGHT_PAREN,		  // 71
	S_COMMA,			      // 72
  S_STAR,             // 73
  S_EQUAL,            // 74
  S_LESS,             // 75
  S_GREATER,          // 76
	IDENT = 85,			    // 85
	INT_LITERAL = 90,	  // 90
  STRING_LITERAL,     // 91
	EOC = 95,			      // 95
	INVALID = 99		    // 99
} token_value;

/* This constants must be updated when add new keywords */
#define TOTAL_KEYWORDS_PLUS_TYPE_NAMES 30

/* New keyword must be added in the same position/order as the enum
   definition above, otherwise the lookup will be wrong */
char *keyword_table[] = 
{
  "int", "varchar", "char", "create", "table", "not", "null", "drop", "list", "schema",
  "for", "to", "insert", "into", "values", "delete", "from", "where", 
  "update", "set", "select", "order", "by", "desc", "is", "and", "or",
  "sum", "avg", "count"
};

/* This enum defines a set of possible statements */
typedef enum s_statement
{
  INVALID_STATEMENT = -199,	// -199
	CREATE_TABLE = 100,				// 100
	DROP_TABLE,								// 101
	LIST_TABLE,								// 102
	LIST_SCHEMA,							// 103
  INSERT,                   // 104
  DELETE,                   // 105
  UPDATE,                   // 106
  SELECT                    // 107
} semantic_statement;

/* This enum has a list of all the errors that should be detected
   by the program.  Can append to this if necessary. */
typedef enum error_return_codes
{
	INVALID_TABLE_NAME = -399,	// -399
	DUPLICATE_TABLE_NAME,				// -398
	TABLE_NOT_EXIST,						// -397
	INVALID_TABLE_DEFINITION,		// -396
	INVALID_COLUMN_NAME,				// -395
	DUPLICATE_COLUMN_NAME,			// -394
	COLUMN_NOT_EXIST,						// -393
	MAX_COLUMN_EXCEEDED,				// -392
	INVALID_TYPE_NAME,					// -391
	INVALID_COLUMN_DEFINITION,	// -390
	INVALID_COLUMN_LENGTH,			// -389
  	INVALID_REPORT_FILE_NAME,		// -388
	INVALID_VALUE,              // -387
	INVALID_VALUES_COUNT,       // -386
	INVALID_AGGREGATE_COLUMN,   // -385
	INVALID_CONDITION,          // -384
	INVALID_CONDITION_OPERAND,  // -383
	MAX_ROW_EXCEEDED,           // -382
	DATA_TYPE_MISMATCH,         // -381
	UNEXPECTED_NULL_VALUE,      // 380
  /* Must add all the possible errors from I/U/D + SELECT here */
	FILE_OPEN_ERROR = -299,			// -299
	DBFILE_CORRUPTION,					// -298
	MEMORY_ERROR,							  // -297
	FILE_REMOVE_ERROR,                     // -296
	TABFILE_CORRUPTION,                    // -295
} return_codes;

/* Table file structures in which we store records of that table */
typedef struct table_file_def {
  int table_file_len;
  int record_len;
  int num_records;
  int offset;
  tpd_entry *tpd_ptr;
} table_file;

typedef enum value_type_def {
  UNKNOWN_FIELD = 0,  // 0
  INT_FIELD,          // 1
  STRING_FIELD        // 2
} value_type;

/* The structure to represent the int/string typed value in each record field.
 */
typedef struct field_value_def {
  int type;
  bool null_type;
  int int_field;
  char string_field[MAX_STRING_LEN + 1];
  token_list *token;
  int col_id;
} field_val;

/* The structure to represent the field value of each record field. */
typedef struct record_field_name_def {
  char name[MAX_IDENT_LEN + 1];
  token_list *token;
} record_field_name;

/* One row as a record of DB */
typedef struct record_row_def {
  int num_fields;
  field_val *value_ptrs[MAX_NUM_COL];
  int sort_column;
  struct record_row_def *next;
} record_row;

/* Condition in record-level predicate by WHERE clause. */
typedef struct record_condition_def {
  int value_type;  // The enum of value_type. It is available only the
                   // operator is in {S_LESS, S_GREATER, S_EQUAL}.
  int col_id;      // LHS operand.
  int op_type;  // Relational operator, can be S_LESS, S_GREATER, S_EQUAL, K_IS
                // (used in "IS NULL"), or K_NOT (used in "IS NOT NULL").
  int int_data_value;  // RHS operand. It is available only if data type is
                       // integer and operator is in {S_LESS, S_GREATER,
                       // S_EQUAL}.
  char string_data_value[MAX_STRING_LEN + 1];  // RHS operand. It is available
                                               // only if data type is string
                                               // and operator is in {S_LESS,
                                               // S_GREATER, S_EQUAL}.
} record_condition;

/* Record predicate represented as WHERE clause. */
typedef struct record_predicate_def {
  int type;  // The relationship of conditions, can be K_AND or K_OR. Set as
             // K_AND by default if there is only one condition.
  int num_conditions;
  record_condition conditions[MAX_NUM_CONDITION];
} record_predicate;

/* Set of function prototypes */
int get_token(char *command, token_list **tok_list);
void add_to_list(token_list **tok_list, char *tmp, int t_class, int t_value);
int do_semantic(token_list *tok_list);
int sem_create_table(token_list *t_list);
int sem_drop_table(token_list *t_list);
int sem_list_tables();
int sem_list_schema(token_list *t_list);
int sem_insert(token_list *t_list);
int sem_select(token_list *t_list);
int sem_delete(token_list *t_list);
int sem_update(token_list *t_list);

/*
	Keep a global list of tpd - in real life, this will be stored
	in shared memory.  Build a set of functions/methods around this.
*/
tpd_list	*g_tpd_list;
int initialize_tpd_list();
int add_tpd_to_list(tpd_entry *tpd);
int drop_tpd_from_list(char *tabname);
tpd_entry* get_tpd_from_list(char *tabname);
int create_tab_file(char *table_name, cd_entry cd_entries[], int num_columns);
int check_insert_values(field_val field_values[], int num_values,
                        cd_entry cd_entries[], int num_columns);
void free_token_list(token_list *const t_list);
int load_table_records(tpd_entry *tpd, table_file **pp_table_header);
int get_file_size(FILE *fhandle);
int fill_raw_record_bytes(cd_entry cd_entries[], field_val *field_values[],
                          int num_cols, char record_bytes[],
                          int num_record_bytes);
int fill_record_row(cd_entry cd_entries[], int num_cols, record_row *p_row,
                    char record_bytes[]);
void print_table_border(cd_entry *sorted_cd_entries[], int num_values);
void print_table_column_names(cd_entry *sorted_cd_entries[],
                              record_field_name field_names[], int num_values);
void print_record_row(cd_entry *sorted_cd_entries[], int num_cols,
                      record_row *row);
void print_aggregate_result(int aggregate_type, int num_fields,
                            int records_count, int int_sum,
                            cd_entry *sorted_cd_entries[]);
int column_display_width(cd_entry *col_entry);
int get_cd_entry_index(cd_entry cd_entries[], int num_cols, char *col_name);
bool apply_row_predicate(cd_entry cd_entries[], int num_cols, record_row *p_row,
                         record_predicate *p_predicate);
bool eval_condition(record_condition *p_condition, field_val *p_field_value);
void sort_records(record_row rows[], int num_records, cd_entry *p_sorting_col,
                  bool is_desc);
int execute_statement(char *statement, int verbose);
int records_comparator(const void *arg1, const void *arg2);
void free_record_row(record_row *row, bool to_last);
int save_records_to_file(table_file *const tab_header,
                         record_row *const rows_head);
int reload_global_tpd_list();
int append_log_with_timestamp(const char *msg, time_t timestamp);
int write_log(const char *msg, bool is_append);
int restore_from_backup_file(char *backup_filename, int db_flags);
int list_tables(tpd_list *table_entries,
                void (*callback)(tpd_entry *table_entry));
void print_table_name(tpd_entry *table_entry);
void remove_table_file(tpd_entry *table_entry);
void rename_table_file(tpd_entry *table_entry);
int update_db_flags(int db_flags);
bool is_timestamp_valid(char *text);
int max_days_of_month(int year, int month);

inline void get_cd_entries(tpd_entry *tab_entry, cd_entry **pp_cd_entry) {
  *pp_cd_entry = (cd_entry *)(((char *)tab_entry) + tab_entry->cd_offset);
}

/* Get pointer to the first record in a table. */
inline void get_table_records(table_file *tab_header, char **pp_record) {
  *pp_record = NULL;
  if (tab_header->table_file_len > tab_header->offset) {
    *pp_record = ((char *)tab_header) + tab_header->offset;
  }
}

/* Check if a token can be an identifier. */
inline bool can_be_identifier(token_list *token) {
  if (!token) {
    return false;
  }
  // Any keyword and type name can also be a valid identifier.
  return (token->tok_class == keyword) ||
         (token->tok_class == identifier) ||
         (token->tok_class == type_name);
}

/* Integer round */
inline int round_integer(int value, int round) {
  int m = value % round;
  return m ? (value + round - m) : value;
}

inline void repeat_print_char(char c, int times) {
  for (int i = 0; i < times; i++) {
    printf("%c", c);
  }
}

inline bool is_file_readable(char *filename, bool is_text_file) {
  FILE *fhandle = fopen(filename, is_text_file ? "r" : "rb");
  if (fhandle) {
    fclose(fhandle);
    return true;
  }
  return false;
}

inline time_t current_timestamp() {
  time_t timestamp;
  // Get time in seconds.
  time(&timestamp);
  return timestamp;
}

inline bool is_a_sql_statement_log_entry(char *raw_text) {
  return isdigit(raw_text[0]) != 0;
}

/* Keep a global list of tpd which will be initialized in db.cpp */
extern tpd_list *g_tpd_list;

#if defined(_WIN32) || defined(_WIN64)
#define strcasecmp _stricmp
#endif

int main(int argc, char** argv)
{
	int rc = 0;
	token_list *tok_list=NULL, *tok_ptr=NULL, *tmp_tok_ptr=NULL;

	if ((argc != 2) || (strlen(argv[1]) == 0))
	{
		printf("Usage: db \"command statement\"\n");
		return 1;
	}

	rc = initialize_tpd_list();

  if (rc)
  {
		printf("\nError in initialize_tpd_list().\nrc = %d\n", rc);
  }
	else
	{
    rc = get_token(argv[1], &tok_list);

		/* Test code */
		tok_ptr = tok_list;
		while (tok_ptr != NULL)
		{
			printf("%16s \t%d \t %d\n",tok_ptr->tok_string, tok_ptr->tok_class,
				      tok_ptr->tok_value);
			tok_ptr = tok_ptr->next;
		}
    
		if (!rc)
		{
			rc = do_semantic(tok_list);
		}

		if (rc)
		{
			tok_ptr = tok_list;
			while (tok_ptr != NULL)
			{
				if ((tok_ptr->tok_class == error) ||
					  (tok_ptr->tok_value == INVALID))
				{
					printf("\nError in the string: %s\n", tok_ptr->tok_string);
					printf("rc=%d\n", rc);
					break;
				}
				tok_ptr = tok_ptr->next;
			}
		}

    /* Whether the token list is valid or not, we need to free the memory */
		tok_ptr = tok_list;
		while (tok_ptr != NULL)
		{
      tmp_tok_ptr = tok_ptr->next;
      free(tok_ptr);
      tok_ptr=tmp_tok_ptr;
		}
	}

	return rc;
}

/************************************************************* 
	This is a lexical analyzer for simple SQL statements
 *************************************************************/
int get_token(char* command, token_list** tok_list)
{
	int rc=0,i,j;
	char *start, *cur, temp_string[MAX_TOK_LEN];
	bool done = false;
	
	start = cur = command;
	while (!done)
	{
		bool found_keyword = false;

		/* This is the TOP Level for each token */
	  memset ((void*)temp_string, '\0', MAX_TOK_LEN);
		i = 0;

		/* Get rid of all the leading blanks */
		while (*cur == ' ')
			cur++;

		if (cur && isalpha(*cur))
		{
			// find valid identifier
			int t_class;
			do 
			{
				temp_string[i++] = *cur++;
			}
			while ((isalnum(*cur)) || (*cur == '_'));

			if (!(strchr(STRING_BREAK, *cur)))
			{
				/* If the next char following the keyword or identifier
				   is not a blank, (, ), or a comma, then append this
					 character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else
			{

				// We have an identifier with at least 1 character
				// Now check if this ident is a keyword
				for (j = 0, found_keyword = false; j < TOTAL_KEYWORDS_PLUS_TYPE_NAMES; j++)
				{
					if ((strcasecmp(keyword_table[j], temp_string) == 0))
					{
						found_keyword = true;
						break;
					}
				}

				if (found_keyword)
				{
				  if (KEYWORD_OFFSET+j < K_CREATE)
						t_class = type_name;
					else if (KEYWORD_OFFSET+j >= F_SUM)
            t_class = function_name;
          else
					  t_class = keyword;

					add_to_list(tok_list, temp_string, t_class, KEYWORD_OFFSET+j);
				}
				else
				{
					if (strlen(temp_string) <= MAX_IDENT_LEN)
					  add_to_list(tok_list, temp_string, identifier, IDENT);
					else
					{
						add_to_list(tok_list, temp_string, error, INVALID);
						rc = INVALID;
						done = true;
					}
				}

				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else if (isdigit(*cur))
		{
			// find valid number
			do 
			{
				temp_string[i++] = *cur++;
			}
			while (isdigit(*cur));

			if (!(strchr(NUMBER_BREAK, *cur)))
			{
				/* If the next char following the keyword or identifier
				   is not a blank or a ), then append this
					 character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else
			{
				add_to_list(tok_list, temp_string, constant, INT_LITERAL);

				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else if ((*cur == '(') || (*cur == ')') || (*cur == ',') || (*cur == '*')
		         || (*cur == '=') || (*cur == '<') || (*cur == '>'))
		{
			/* Catch all the symbols here. Note: no look ahead here. */
			int t_value;
			switch (*cur)
			{
				case '(' : t_value = S_LEFT_PAREN; break;
				case ')' : t_value = S_RIGHT_PAREN; break;
				case ',' : t_value = S_COMMA; break;
				case '*' : t_value = S_STAR; break;
				case '=' : t_value = S_EQUAL; break;
				case '<' : t_value = S_LESS; break;
				case '>' : t_value = S_GREATER; break;
			}

			temp_string[i++] = *cur++;

			add_to_list(tok_list, temp_string, symbol, t_value);

			if (!*cur)
			{
				add_to_list(tok_list, "", terminator, EOC);
				done = true;
			}
		}
    else if (*cur == '\'')
    {
      /* Find STRING_LITERRAL */
			int t_class;
      cur++;
			do 
			{
				temp_string[i++] = *cur++;
			}
			while ((*cur) && (*cur != '\''));

      temp_string[i] = '\0';

			if (!*cur)
			{
				/* If we reach the end of line */
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
      else /* must be a ' */
      {
        add_to_list(tok_list, temp_string, constant, STRING_LITERAL);
        cur++;
				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
        }
      }
    }
		else
		{
			if (!*cur)
			{
				add_to_list(tok_list, "", terminator, EOC);
				done = true;
			}
			else
			{
				/* not a ident, number, or valid symbol */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
		}
	}
			
  return rc;
}

void add_to_list(token_list **tok_list, char *tmp, int t_class, int t_value)
{
	token_list *cur = *tok_list;
	token_list *ptr = NULL;

	// printf("%16s \t%d \t %d\n",tmp, t_class, t_value);

	ptr = (token_list*)calloc(1, sizeof(token_list));
	strcpy(ptr->tok_string, tmp);
	ptr->tok_class = t_class;
	ptr->tok_value = t_value;
	ptr->next = NULL;

  if (cur == NULL)
		*tok_list = ptr;
	else
	{
		while (cur->next != NULL)
			cur = cur->next;

		cur->next = ptr;
	}
	return;
}

int do_semantic(token_list *tok_list)
{
	int rc = 0, cur_cmd = INVALID_STATEMENT;
	bool unique = false;
  token_list *cur = tok_list;

	if ((cur->tok_value == K_CREATE) &&
			((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("CREATE TABLE statement\n");
		cur_cmd = CREATE_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_DROP) &&
					((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("DROP TABLE statement\n");
		cur_cmd = DROP_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_LIST) &&
					((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("LIST TABLE statement\n");
		cur_cmd = LIST_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_LIST) &&
					((cur->next != NULL) && (cur->next->tok_value == K_SCHEMA)))
	{
		printf("LIST SCHEMA statement\n");
		cur_cmd = LIST_SCHEMA;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_INSERT) &&
					((cur->next != NULL) && (cur->next->tok_value == K_INTO)))
	{
		printf("INSERT INTO statement\n");
		cur_cmd = INSERT;
		cur = cur->next->next;
	} else if (cur->tok_value == K_SELECT) {
		printf("SELECT statement\n");
		cur_cmd = SELECT;
		cur = cur->next;
	} else if (cur->tok_value == K_UPDATE) {
		printf("UPDATE statement\n");
		cur_cmd = UPDATE;
		cur = cur->next;
	} else if (cur->tok_value == K_DELETE) {
		printf("DELETE statement\n");
		cur_cmd = DELETE;
		cur = cur->next->next;
	}
  
	else
  {
		printf("Invalid statement\n");
		rc = cur_cmd;
	}

	if (cur_cmd != INVALID_STATEMENT)
	{
		switch(cur_cmd)
		{
			case CREATE_TABLE:
						rc = sem_create_table(cur);
						break;
			case DROP_TABLE:
						rc = sem_drop_table(cur);
						break;
			case LIST_TABLE:
						rc = sem_list_tables();
						break;
			case LIST_SCHEMA:
						rc = sem_list_schema(cur);
						break;
			case INSERT:
						rc = sem_insert(cur);
						break;
			case SELECT:
						rc = sem_select(cur);
						break;
			case UPDATE:
						rc = sem_update(cur);
						break;
			case DELETE:
						rc = sem_delete(cur);
						break;
			default:
					; /* no action */
		}
	}
	
	return rc;
}

int sem_create_table(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry tab_entry;
	tpd_entry *new_entry = NULL;
	bool column_done = false;
	int cur_id = 0;
	cd_entry	col_entry[MAX_NUM_COL];


	memset(&tab_entry, '\0', sizeof(tpd_entry));
	cur = t_list;
	if ((cur->tok_class != keyword) &&
		  (cur->tok_class != identifier) &&
			(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if ((new_entry = get_tpd_from_list(cur->tok_string)) != NULL)
		{
			rc = DUPLICATE_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			strcpy(tab_entry.table_name, cur->tok_string);
			cur = cur->next;
			if (cur->tok_value != S_LEFT_PAREN)
			{
				//Error
				rc = INVALID_TABLE_DEFINITION;
				cur->tok_value = INVALID;
			}
			else
			{
				memset(&col_entry, '\0', (MAX_NUM_COL * sizeof(cd_entry)));

				/* Now build a set of column entries */
				cur = cur->next;
				do
				{
					if ((cur->tok_class != keyword) &&
							(cur->tok_class != identifier) &&
							(cur->tok_class != type_name))
					{
						// Error
						rc = INVALID_COLUMN_NAME;
						cur->tok_value = INVALID;
					}
					else
					{
						int i;
						for(i = 0; i < cur_id; i++)
						{
              /* make column name case sensitive */
							if (strcmp(col_entry[i].col_name, cur->tok_string)==0)
							{
								rc = DUPLICATE_COLUMN_NAME;
								cur->tok_value = INVALID;
								break;
							}
						}

						if (!rc)
						{
							strcpy(col_entry[cur_id].col_name, cur->tok_string);
							col_entry[cur_id].col_id = cur_id;
							col_entry[cur_id].not_null = false;    /* set default */

							cur = cur->next;
							if (cur->tok_class != type_name)
							{
								// Error
								rc = INVALID_TYPE_NAME;
								cur->tok_value = INVALID;
							}
							else
							{
                /* Set the column type here, int or char */
								col_entry[cur_id].col_type = cur->tok_value;
								cur = cur->next;
		
								if (col_entry[cur_id].col_type == T_INT)
								{
									if ((cur->tok_value != S_COMMA) &&
										  (cur->tok_value != K_NOT) &&
										  (cur->tok_value != S_RIGHT_PAREN))
									{
										rc = INVALID_COLUMN_DEFINITION;
										cur->tok_value = INVALID;
									}
								  else
									{
										col_entry[cur_id].col_len = sizeof(int);
										
										if ((cur->tok_value == K_NOT) &&
											  (cur->next->tok_value != K_NULL))
										{
											rc = INVALID_COLUMN_DEFINITION;
											cur->tok_value = INVALID;
										}	
										else if ((cur->tok_value == K_NOT) &&
											    (cur->next->tok_value == K_NULL))
										{					
											col_entry[cur_id].not_null = true;
											cur = cur->next->next;
										}
	
										if (!rc)
										{
											/* I must have either a comma or right paren */
											if ((cur->tok_value != S_RIGHT_PAREN) &&
												  (cur->tok_value != S_COMMA))
											{
												rc = INVALID_COLUMN_DEFINITION;
												cur->tok_value = INVALID;
											}
											else
		                  {
												if (cur->tok_value == S_RIGHT_PAREN)
												{
 													column_done = true;
												}
												cur = cur->next;
											}
										}
									}
								}   // end of T_INT processing
								else
								{
									// It must be char() or varchar() 
									if (cur->tok_value != S_LEFT_PAREN)
									{
										rc = INVALID_COLUMN_DEFINITION;
										cur->tok_value = INVALID;
									}
									else
									{
										/* Enter char(n) processing */
										cur = cur->next;
		
										if (cur->tok_value != INT_LITERAL)
										{
											rc = INVALID_COLUMN_LENGTH;
											cur->tok_value = INVALID;
										}
										else
										{
											/* Got a valid integer - convert */
											col_entry[cur_id].col_len = atoi(cur->tok_string);
											cur = cur->next;
											
											if (cur->tok_value != S_RIGHT_PAREN)
											{
												rc = INVALID_COLUMN_DEFINITION;
												cur->tok_value = INVALID;
											}
											else
											{
												cur = cur->next;
						
												if ((cur->tok_value != S_COMMA) &&
														(cur->tok_value != K_NOT) &&
														(cur->tok_value != S_RIGHT_PAREN))
												{
													rc = INVALID_COLUMN_DEFINITION;
													cur->tok_value = INVALID;
												}
												else
												{
													if ((cur->tok_value == K_NOT) &&
														  (cur->next->tok_value != K_NULL))
													{
														rc = INVALID_COLUMN_DEFINITION;
														cur->tok_value = INVALID;
													}
													else if ((cur->tok_value == K_NOT) &&
																	 (cur->next->tok_value == K_NULL))
													{					
														col_entry[cur_id].not_null = true;
														cur = cur->next->next;
													}
		
													if (!rc)
													{
														/* I must have either a comma or right paren */
														if ((cur->tok_value != S_RIGHT_PAREN) &&															  (cur->tok_value != S_COMMA))
														{
															rc = INVALID_COLUMN_DEFINITION;
															cur->tok_value = INVALID;
														}
														else
													  {
															if (cur->tok_value == S_RIGHT_PAREN)
															{
																column_done = true;
															}
															cur = cur->next;
														}
													}
												}
											}
										}	/* end char(n) processing */
									}
								} /* end char processing */
							}
						}  // duplicate column name
					} // invalid column name

					/* If rc=0, then get ready for the next column */
					if (!rc)
					{
						cur_id++;
					}

				} while ((rc == 0) && (!column_done));
	
				if ((column_done) && (cur->tok_value != EOC))
				{
					rc = INVALID_TABLE_DEFINITION;
					cur->tok_value = INVALID;
				}

				if (!rc)
				{
					/* Now finished building tpd and add it to the tpd list */
					tab_entry.num_columns = cur_id;
					tab_entry.tpd_size = sizeof(tpd_entry) + 
															 sizeof(cd_entry) *	tab_entry.num_columns;
				  tab_entry.cd_offset = sizeof(tpd_entry);
					new_entry = (tpd_entry*)calloc(1, tab_entry.tpd_size);

					if (new_entry == NULL)
					{
						rc = MEMORY_ERROR;
					}
					else
					{
						memcpy((void*)new_entry,
							     (void*)&tab_entry,
									 sizeof(tpd_entry));
		
						memcpy((void*)((char*)new_entry + sizeof(tpd_entry)),
									 (void*)col_entry,
									 sizeof(cd_entry) * tab_entry.num_columns);
	
						rc = add_tpd_to_list(new_entry);

						// Create .tab file.
						if (!rc) {
						rc = create_tab_file(tab_entry.table_name, col_entry,
											tab_entry.num_columns);
						if (rc) {
							cur->tok_value = INVALID;
						}
						}

						free(new_entry);
					}
				}
			}
		}
	}
  return rc;
}

int sem_drop_table(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;

	cur = t_list;
	if ((cur->tok_class != keyword) &&
		  (cur->tok_class != identifier) &&
			(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if (cur->next->tok_value != EOC)
		{
			rc = INVALID_STATEMENT;
			cur->next->tok_value = INVALID;
		}
		else
		{
			if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
			{
				rc = TABLE_NOT_EXIST;
				cur->tok_value = INVALID;
			}
			else
			{
				/* Found a valid tpd, drop it from tpd list */
				rc = drop_tpd_from_list(cur->tok_string);
			}
		}
	}

  return rc;
}

int sem_list_tables()
{
	int rc = 0;
	int num_tables = g_tpd_list->num_tables;
	tpd_entry *cur = &(g_tpd_list->tpd_start);

	if (num_tables == 0)
	{
		printf("\nThere are currently no tables defined\n");
	}
	else
	{
		printf("\nTable List\n");
		printf("*****************\n");
		while (num_tables-- > 0)
		{
			printf("%s\n", cur->table_name);
			if (num_tables > 0)
			{
				cur = (tpd_entry*)((char*)cur + cur->tpd_size);
			}
		}
		printf("****** End ******\n");
	}

  return rc;
}

int sem_list_schema(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	cd_entry  *col_entry = NULL;
	char tab_name[MAX_IDENT_LEN+1];
	char filename[MAX_IDENT_LEN+1];
	bool report = false;
	FILE *fhandle = NULL;
	int i = 0;

	cur = t_list;

	if (cur->tok_value != K_FOR)
  {
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
	}
	else
	{
		cur = cur->next;

		if ((cur->tok_class != keyword) &&
			  (cur->tok_class != identifier) &&
				(cur->tok_class != type_name))
		{
			// Error
			rc = INVALID_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			memset(filename, '\0', MAX_IDENT_LEN+1);
			strcpy(tab_name, cur->tok_string);
			cur = cur->next;

			if (cur->tok_value != EOC)
			{
				if (cur->tok_value == K_TO)
				{
					cur = cur->next;
					
					if ((cur->tok_class != keyword) &&
						  (cur->tok_class != identifier) &&
							(cur->tok_class != type_name))
					{
						// Error
						rc = INVALID_REPORT_FILE_NAME;
						cur->tok_value = INVALID;
					}
					else
					{
						if (cur->next->tok_value != EOC)
						{
							rc = INVALID_STATEMENT;
							cur->next->tok_value = INVALID;
						}
						else
						{
							/* We have a valid file name */
							strcpy(filename, cur->tok_string);
							report = true;
						}
					}
				}
				else
				{ 
					/* Missing the TO keyword */
					rc = INVALID_STATEMENT;
					cur->tok_value = INVALID;
				}
			}

			if (!rc)
			{
				if ((tab_entry = get_tpd_from_list(tab_name)) == NULL)
				{
					rc = TABLE_NOT_EXIST;
					cur->tok_value = INVALID;
				}
				else
				{
					if (report)
					{
						if((fhandle = fopen(filename, "a+tc")) == NULL)
						{
							rc = FILE_OPEN_ERROR;
						}
					}

					if (!rc)
					{
						/* Find correct tpd, need to parse column and index information */

						/* First, write the tpd_entry information */
						printf("Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
						printf("Table Name               (table_name)  = %s\n", tab_entry->table_name);
						printf("Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
						printf("Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
            printf("Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags); 

						if (report)
						{
							fprintf(fhandle, "Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
							fprintf(fhandle, "Table Name               (table_name)  = %s\n", tab_entry->table_name);
							fprintf(fhandle, "Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
							fprintf(fhandle, "Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
              fprintf(fhandle, "Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags); 
						}

						/* Next, write the cd_entry information */
						for(i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
								i < tab_entry->num_columns; i++, col_entry++)
						{
							printf("Column Name   (col_name) = %s\n", col_entry->col_name);
							printf("Column Id     (col_id)   = %d\n", col_entry->col_id);
							printf("Column Type   (col_type) = %d\n", col_entry->col_type);
							printf("Column Length (col_len)  = %d\n", col_entry->col_len);
							printf("Not Null flag (not_null) = %d\n\n", col_entry->not_null);

							if (report)
							{
								fprintf(fhandle, "Column Name   (col_name) = %s\n", col_entry->col_name);
								fprintf(fhandle, "Column Id     (col_id)   = %d\n", col_entry->col_id);
								fprintf(fhandle, "Column Type   (col_type) = %d\n", col_entry->col_type);
								fprintf(fhandle, "Column Length (col_len)  = %d\n", col_entry->col_len);
								fprintf(fhandle, "Not Null Flag (not_null) = %d\n\n", col_entry->not_null);
							}
						}
	
						if (report)
						{
							fflush(fhandle);
							fclose(fhandle);
						}
					} // File open error							
				} // Table not exist
			} // no semantic errors
		} // Invalid table name
	} // Invalid statement

  return rc;
}

int sem_insert(token_list *t_list) {
	int rc = 0;
	token_list *cur = t_list;
	printf("heree-1");
	if (!can_be_identifier(cur)) {
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
		return rc;
	}

	tpd_entry *tab_entry = get_tpd_from_list(cur->tok_string);

	if (tab_entry == NULL) {
		rc = TABLE_NOT_EXIST;
		cur->tok_value = INVALID;
		return rc;
	}

	cur = cur->next;
	if ((cur->tok_value != K_VALUES) || (cur->next->tok_value != S_LEFT_PAREN)) {
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
		return rc;
	}

	cur = cur->next->next;

	field_val *field_values = (field_val *)calloc(MAX_NUM_COL, sizeof(field_val));
	int num_values = 0;
	while (true) {
		// Check if values count is greater than MAX_NUM_COL.
		if (num_values >= MAX_NUM_COL) {
			rc = MAX_COLUMN_EXCEEDED;
			cur->tok_value = INVALID;
			return rc;
		}

		if (cur->tok_value == STRING_LITERAL) {
			field_values[num_values].type = STRING_FIELD;
			field_values[num_values].null_type = false;
			strcpy(field_values[num_values].string_field, cur->tok_string);
			field_values[num_values].token = cur;
			field_values[num_values].col_id = num_values;
		}
		else if (cur->tok_value == INT_LITERAL) {
			field_values[num_values].type = INT_FIELD;
			field_values[num_values].null_type = false;
			field_values[num_values].int_field = atoi(cur->tok_string);
			field_values[num_values].token = cur;
			field_values[num_values].col_id = num_values;
		}
		else if (cur->tok_value == K_NULL) {
			field_values[num_values].type = UNKNOWN_FIELD;
			field_values[num_values].null_type = true;
			field_values[num_values].token = cur;
			field_values[num_values].col_id = num_values;
		}
		else {
			rc = INVALID_VALUE;
			break;
		}
		cur = cur->next;
		if (cur->tok_value == S_COMMA) {
		}
		else if (cur->tok_value == S_RIGHT_PAREN) {
			cur = cur->next;
			num_values++;
			break;
		}
		else {
			rc = INVALID_STATEMENT;
			break;
		}
		cur = cur->next;
		num_values++;
	}
	printf("heree-2");
	if (cur->tok_value != EOC) {
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
		return rc;
	}

	if (rc) {
		cur->tok_value = INVALID;
		return rc;
	}

	cd_entry *cd_entries = NULL;
	get_cd_entries(tab_entry, &cd_entries);
	// The order and number of columns must be the same for field_values and
	// cd_entries.
	rc = check_insert_values(field_values, num_values, cd_entries,
		tab_entry->num_columns);

	if (rc) {
		cur->tok_value = INVALID;
		return rc;
	}
	printf("heree2");
	// It is the heap memory owner of the content of the whole table.
	// Do not forget to free it later.
	table_file *tab_header = NULL;
	if ((rc = load_table_records(tab_entry, &tab_header)) != 0) {
		
	printf("heree3");
		return rc;
	}
	if (tab_header->num_records >= MAX_NUM_ROW) {
		rc = MAX_ROW_EXCEEDED;
		free(tab_header);
		return rc;
	}
	printf("heree4");
	// Compose the new record.
	char *record_bytes = (char *)malloc(tab_header->record_len);
	field_val *field_value_ptrs[MAX_NUM_COL];
	for (int i = 0; i < num_values; i++) {
		field_value_ptrs[i] = &field_values[i];
	}
	fill_raw_record_bytes(cd_entries, field_value_ptrs, num_values, record_bytes,
		tab_header->record_len);
	printf("heree0");
	// Append the new record to the .tab file.
	char table_filename[MAX_IDENT_LEN + 5];
	sprintf(table_filename, "%s.tab", tab_header->tpd_ptr->table_name);
	FILE *fhandle = NULL;
	if ((fhandle = fopen(table_filename, "wbc")) == NULL) {
		rc = FILE_OPEN_ERROR;
	}
	else {
		// Add one more record in the table header.
		int old_table_file_size = tab_header->table_file_len;
		tab_header->num_records++;
		tab_header->table_file_len += tab_header->record_len;
		tab_header->tpd_ptr = NULL;  // Reset tpd pointer.

		fwrite(tab_header, old_table_file_size, 1, fhandle);
		fwrite(record_bytes, tab_header->record_len, 1, fhandle);
		fflush(fhandle);
		fclose(fhandle);
	}
	free(record_bytes);
	free(tab_header);
	return rc;
}


int sem_select(token_list *t_list) {
	int rc = 0;
	token_list *cur = t_list;

	record_field_name field_names[MAX_NUM_COL];
	bool fields_done = false;
	int wildcard_field_index = -1;
	int num_fields = 0;

	// Try parse aggregate operator: SUM, AVG, or COUNT.
	int aggregate_type = 0;
	if (cur->tok_class == function_name && cur->next->tok_value == S_LEFT_PAREN) {
		aggregate_type = cur->tok_value;  // Can be F_COUNT, F_SUM, or F_AVG.
		if (!(can_be_identifier(cur->next->next) || (cur->next->next != NULL && cur->next->next->tok_value == S_STAR))) {
			// Error column name.
			rc = INVALID_COLUMN_NAME;
			cur->tok_value = INVALID;
			return rc;
		}
		cur = cur->next->next;
		strcpy(field_names[num_fields].name, cur->tok_string);
		field_names[num_fields].token = cur;
		if (strcmp(cur->tok_string, "*") == 0) {
			wildcard_field_index = 0;
		}
		cur = cur->next;
		if (cur->tok_value != S_RIGHT_PAREN) {
			// Error column name.
			rc = INVALID_COLUMN_NAME;
			cur->tok_value = INVALID;
			return rc;
		} else {
			cur = cur->next;
			num_fields++;
			fields_done = true;
		}
	}

	// Extract field names. However, it will be skipped if the unique aggregate
	// field is found.
	while (!fields_done) {
		if (!(can_be_identifier(cur) || cur->tok_value == S_STAR)) {
			// Error column name.
			rc = INVALID_COLUMN_NAME;
			cur->tok_value = INVALID;
			return rc;
		}

		// Check if wildcard has already been caught.
		if (wildcard_field_index == 0) {
			rc = INVALID_COLUMN_NAME;
			cur->tok_value = INVALID;
			return rc;
		}

		if (strcmp(cur->tok_string, "*") == 0) {
			// Wildcard must appear only once, as the first column name.
			if (wildcard_field_index == -1 && num_fields == 0) {
				wildcard_field_index = 0;
			} else {
				rc = INVALID_COLUMN_NAME;
				cur->tok_value = INVALID;
				return rc;
			}
		}
		strcpy(field_names[num_fields].name, cur->tok_string);
		field_names[num_fields].token = cur;
		num_fields++;
		cur = cur->next;
		if (cur->tok_value == S_COMMA) {
			cur = cur->next;
		} else {
			fields_done = true;
		}
	}

	if (cur->tok_value != K_FROM) {
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
		return rc;
	}

	cur = cur->next;
	if (!can_be_identifier(cur)) {
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
		return rc;
	}

	// Check whether the table name exists.
	tpd_entry *tab_entry = get_tpd_from_list(cur->tok_string);
	if (tab_entry == NULL) {
		rc = TABLE_NOT_EXIST;
		cur->tok_value = INVALID;
		return rc;
	}

	// Get column descriptors.
	cd_entry *cd_entries = NULL;
	get_cd_entries(tab_entry, &cd_entries);

	// Expand wildcard field to all field names of the table.
	if (wildcard_field_index == 0) {
		token_list *wildcard_token = field_names[wildcard_field_index].token;
		num_fields = tab_entry->num_columns;
		for (int i = 0; i < num_fields; i++) {
			strcpy(field_names[i].name, cd_entries[i].col_name);
			field_names[i].token = wildcard_token;
		}
	}

	// Check if all field names exist in that table and generate
	// sorted_cd_entries.
	cd_entry *sorted_cd_entries[MAX_NUM_COL];
	for (int i = 0; i < num_fields; i++) {
		int col_index = get_cd_entry_index(cd_entries, tab_entry->num_columns, field_names[i].name);
		if (col_index < 0) {
			rc = INVALID_COLUMN_NAME;
			field_names[i].token->tok_value = INVALID;
			return rc;
		} else {
			// SUM and AVG aggregate functions are only valid on the unique interger
			// column.
			if ((aggregate_type == F_SUM) || (aggregate_type == F_AVG)) {
				if ((num_fields == 1) && (cd_entries[col_index].col_type == T_INT)) {
					sorted_cd_entries[i] = &cd_entries[col_index];
				} else {
					rc = INVALID_AGGREGATE_COLUMN;
					field_names[i].token->tok_value = INVALID;
					return rc;
				}
			} else {
				sorted_cd_entries[i] = &cd_entries[col_index];
			}
		}
	}

	bool has_where_clause = false;
	record_predicate row_filter;
	memset(&row_filter, '\0', sizeof(record_predicate));
	row_filter.type = K_AND;  // Set default relationship of conditions.

	cur = cur->next;
	// Parse optional WHERE clause.
	if (cur->tok_value == K_WHERE) {
		has_where_clause = true;

		int col_index = -1;
		int num_conditions = 0;
		bool has_more_condition = true;

		while (has_more_condition) {
			// Read column name.
			cur = cur->next;
			if (can_be_identifier(cur)) {
				col_index = get_cd_entry_index(cd_entries, tab_entry->num_columns, cur->tok_string);
				if (col_index > -1) {
					row_filter.conditions[num_conditions].col_id = col_index;
					row_filter.conditions[num_conditions].value_type =
						((cd_entries[col_index].col_type == T_INT)
							? INT_FIELD
							: STRING_FIELD);
				} else {
					rc = INVALID_COLUMN_NAME;
					cur->tok_value = INVALID;
					return rc;
				}
			} else {
				rc = INVALID_COLUMN_NAME;
				cur->tok_value = INVALID;
				return rc;
			}

			// Read relational operator of first condition.
			cur = cur->next;
			if (cur->tok_value == S_LESS || cur->tok_value == S_GREATER || cur->tok_value == S_EQUAL) {
				row_filter.conditions[num_conditions].op_type = cur->tok_value;
				cur = cur->next;
				if (cur->tok_value == INT_LITERAL) {
					if (row_filter.conditions[num_conditions].value_type == INT_FIELD) {
						row_filter.conditions[num_conditions].int_data_value = atoi(cur->tok_string);
					} else {
						rc = INVALID_CONDITION_OPERAND;
						cur->tok_value = INVALID;
						return rc;
					}
				} else if (cur->tok_value == STRING_LITERAL) {
					if (row_filter.conditions[num_conditions].value_type == STRING_FIELD) {
						strcpy(row_filter.conditions[num_conditions].string_data_value, cur->tok_string);
					} else {
						rc = INVALID_CONDITION_OPERAND;
						cur->tok_value = INVALID;
						return rc;
					}
				} else {
					rc = INVALID_CONDITION;
					cur->tok_value = INVALID;
					return rc;
				}
			} else if (cur->tok_value == K_IS && cur->next->tok_value == K_NULL) {  // "IS NULL"
				cur = cur->next;
				row_filter.conditions[num_conditions].op_type = K_IS;
			} else if (cur->tok_value == K_IS && cur->next->tok_value == K_NOT && cur->next->next->tok_value == K_NULL) {  // "IS NOT NULL"
				cur = cur->next->next;
				row_filter.conditions[num_conditions].op_type = K_NOT;
			} else {
				rc = INVALID_CONDITION;
				cur->tok_value = INVALID;
				return rc;
			}
			num_conditions++;
			row_filter.num_conditions = num_conditions;

			if (num_conditions == MAX_NUM_CONDITION) {
				break;
			}

			if (cur->next->tok_value == K_AND || cur->next->tok_value == K_OR) {
				cur = cur->next;
				has_more_condition = true;
				row_filter.type = cur->tok_value;
			} else {
				has_more_condition = false;
			}
		}  // End of while.
		cur = cur->next;
	}

	// Parse ORDER BY clause
	bool has_order_by_clause = false;
	bool order_by_desc = false;
	int order_by_column_id = -1;

	if (cur->tok_value == K_ORDER && cur->next->tok_value == K_BY) {
		cur = cur->next->next;
		if (can_be_identifier(cur)) {
			order_by_column_id = get_cd_entry_index(cd_entries, tab_entry->num_columns, cur->tok_string);
			if (order_by_column_id > -1) {
				has_order_by_clause = true;
				cur = cur->next;
				if (cur->tok_value == K_DESC) {
					order_by_desc = true;
					cur = cur->next;
				}
			} else {
				rc = INVALID_COLUMN_NAME;
				cur->tok_value = INVALID;
				return rc;
			}
		} else {
			rc = INVALID_COLUMN_NAME;
			cur->tok_value = INVALID;
			return rc;
		}
	}

	if (cur->tok_value != EOC) {
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
		return rc;
	}

	// It is the heap memory owner of the content of the whole table.
	// Do not forget to free it later.
	table_file *tab_header = NULL;
	if ((rc = load_table_records(tab_entry, &tab_header)) != 0) {
		return rc;
	}

	// Load record rows.
	char *record_in_table = NULL;
	get_table_records(tab_header, &record_in_table);
	int aggregate_int_sum = 0;
	int num_loaded_records = 0;
	int aggregate_records_count = 0;

	record_row record_rows[MAX_NUM_ROW];
	memset(record_rows, '\0', sizeof(record_rows));
	record_row *p_current_row = NULL;
	for (int i = 0; i < tab_header->num_records; i++) {
		// Fill all field values (not only displayed columns) from current record.
		p_current_row = &record_rows[num_loaded_records];
		fill_record_row(cd_entries, tab_entry->num_columns, p_current_row, record_in_table);

		// Do filtering on current record row.
		if (has_where_clause && (!apply_row_predicate(cd_entries, tab_entry->num_columns, p_current_row, &row_filter))) {
			// Current row is not qualified so should be skipped (i.e. will not be
			// loaded in final result set).
			for (int j = 0; j < p_current_row->num_fields; j++) {
				free(p_current_row->value_ptrs[j]);
			}
			memset(p_current_row, '\0', sizeof(record_row));
		} else {
			/* Current row survives. */
			num_loaded_records++;

			if ((aggregate_type == F_SUM) || (aggregate_type == F_AVG)) {
				// SUM(col) or AVG(col), ignore NULL rows.
				if (!p_current_row->value_ptrs[sorted_cd_entries[0]->col_id]->null_type) {
					aggregate_int_sum += p_current_row->value_ptrs[sorted_cd_entries[0]->col_id]->int_field;
					aggregate_records_count++;
				}
			} else if (aggregate_type == F_COUNT) {
				if (num_fields == 1) {
					// count(col), ignore NULL rows.
					if (!p_current_row->value_ptrs[sorted_cd_entries[0]->col_id]->null_type) {
						aggregate_records_count++;
					}
				} else {
					// count(*), include NULL rows.
					aggregate_records_count++;
				}
			}
		}

		// Move forward to next record.
		record_in_table += tab_header->record_len;
	}
	if (aggregate_type == 0) {
		print_table_border(sorted_cd_entries, num_fields);
		print_table_column_names(sorted_cd_entries, field_names, num_fields);
		print_table_border(sorted_cd_entries, num_fields);

		// Sort records if necessary.
		if (has_order_by_clause) {
			sort_records(record_rows, num_loaded_records, &cd_entries[order_by_column_id], order_by_desc);
		}

		// Print all sorted records.
		for (int i = 0; i < num_loaded_records; i++) {
			print_record_row(sorted_cd_entries, num_fields, &record_rows[i]);
		}
		print_table_border(sorted_cd_entries, num_fields);
	} else {
		// Aggregate result is shown as a 1x1 table.
		print_aggregate_result(aggregate_type, num_fields, aggregate_records_count, aggregate_int_sum, sorted_cd_entries);
	}

	// Clean allocated heap memory.
	free(tab_header);
	for (int i = 0; i < num_loaded_records; i++) {
		for (int j = 0; j < record_rows[i].num_fields; j++) {
			free(record_rows[i].value_ptrs[j]);
		}
	}
	return rc;
}

int sem_update(token_list *t_list) {
	int rc = 0;
	token_list *cur = t_list;

	if (!can_be_identifier(cur)) {
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
		return rc;
	}

	// Check whether the table name exists.
	tpd_entry *tab_entry = get_tpd_from_list(cur->tok_string);
	if (tab_entry == NULL) {
		rc = TABLE_NOT_EXIST;
		cur->tok_value = INVALID;
		return rc;
	}

	// Get column descriptors.
	cd_entry *cd_entries = NULL;
	get_cd_entries(tab_entry, &cd_entries);

	cur = cur->next;
	if (cur->tok_value != K_SET) {
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
		return rc;
	}

	// Parse field name of the value to be updated.
	cur = cur->next;
	field_val value_to_update;
	bool value_to_update_can_be_null = false;
	memset(&value_to_update, '\0', sizeof(value_to_update));
	if (can_be_identifier(cur)) {
		int col_index =
			get_cd_entry_index(cd_entries, tab_entry->num_columns, cur->tok_string);
		if (col_index > -1) {
			value_to_update.col_id = col_index;
			value_to_update.token = cur;
			value_to_update.type =
				((cd_entries[col_index].col_type == T_INT) ? INT_FIELD
					: STRING_FIELD);
			value_to_update_can_be_null = !cd_entries[col_index].not_null;
		}
		else {
			rc = INVALID_COLUMN_NAME;
			cur->tok_value = INVALID;
			return rc;
		}
	}
	else {
		rc = INVALID_COLUMN_NAME;
		cur->tok_value = INVALID;
		return rc;
	}

	cur = cur->next;
	if (cur->tok_value != S_EQUAL) {
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
		return rc;
	}

	// Parse field value of the value to be updated.
	cur = cur->next;
	if (cur->tok_value == STRING_LITERAL) {
		if (value_to_update.type == STRING_FIELD) {
			strcpy(value_to_update.string_field, cur->tok_string);
			value_to_update.null_type = false;
		}
		else {
			rc = DATA_TYPE_MISMATCH;
			cur->tok_value = INVALID;
			return rc;
		}
	}
	else if (cur->tok_value == INT_LITERAL) {
		if (value_to_update.type == INT_FIELD) {
			value_to_update.int_field = atoi(cur->tok_string);
			value_to_update.null_type = false;
		}
		else {
			rc = DATA_TYPE_MISMATCH;
			cur->tok_value = INVALID;
			return rc;
		}
	}
	else if (cur->tok_value == K_NULL) {
		if (value_to_update_can_be_null) {
			value_to_update.null_type = true;
		}
		else {
			rc = UNEXPECTED_NULL_VALUE;
			cur->tok_value = INVALID;
			return rc;
		}
	}
	else {
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
		return rc;
	}

	bool has_where_clause = false;
	record_predicate row_filter;
	memset(&row_filter, '\0', sizeof(row_filter));
	// Parse WHERE clause.
	cur = cur->next;
	if (cur->tok_value == K_WHERE) {
		has_where_clause = true;
		row_filter.type = K_AND;
		row_filter.num_conditions = 1;

		// Parse name of the filtering column.
		cur = cur->next;
		if (can_be_identifier(cur)) {
			int col_index = get_cd_entry_index(cd_entries, tab_entry->num_columns,
				cur->tok_string);
			if (col_index > -1) {
				row_filter.conditions[0].col_id = col_index;
				row_filter.conditions[0].value_type =
					((cd_entries[col_index].col_type == T_INT)
						? INT_FIELD
						: STRING_FIELD);
			}
			else {
				rc = INVALID_COLUMN_NAME;
				cur->tok_value = INVALID;
				return rc;
			}
		}
		else {
			rc = INVALID_COLUMN_NAME;
			cur->tok_value = INVALID;
			return rc;
		}

		// Parse the operator and operand of the condition.
		cur = cur->next;
		if (cur->tok_value == S_LESS || cur->tok_value == S_GREATER ||
			cur->tok_value == S_EQUAL) {
			row_filter.conditions[0].op_type = cur->tok_value;
			cur = cur->next;
			if (cur->tok_value == INT_LITERAL) {
				if (row_filter.conditions[0].value_type == INT_FIELD) {
					row_filter.conditions[0].int_data_value = atoi(cur->tok_string);
				}
				else {
					rc = INVALID_CONDITION_OPERAND;
					cur->tok_value = INVALID;
					return rc;
				}
			}
			else if (cur->tok_value == STRING_LITERAL) {
				if (row_filter.conditions[0].value_type == STRING_FIELD) {
					strcpy(row_filter.conditions[0].string_data_value, cur->tok_string);
				}
				else {
					rc = INVALID_CONDITION_OPERAND;
					cur->tok_value = INVALID;
					return rc;
				}
			}
			else {
				rc = INVALID_CONDITION;
				cur->tok_value = INVALID;
				return rc;
			}
		}
		else if (cur->tok_value == K_IS &&
			cur->next->tok_value == K_NULL) {  // "IS NULL"
			cur = cur->next;
			row_filter.conditions[0].op_type = K_IS;
		}
		else if (cur->tok_value == K_IS && cur->next->tok_value == K_NOT &&
			cur->next->next->tok_value == K_NULL) {  // "IS NOT NULL"
			cur = cur->next->next;
			row_filter.conditions[0].op_type = K_NOT;
		}
		else {
			rc = INVALID_CONDITION;
			cur->tok_value = INVALID;
			return rc;
		}
		cur = cur->next;
	}

	if (cur->tok_value != EOC) {
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
		return rc;
	}

	// It is the heap memory owner of the content of the whole table.
	// Do not forget to free it later.
	table_file *tab_header = NULL;
	if ((rc = load_table_records(tab_entry, &tab_header)) != 0) {
		return rc;
	}

	// Load record rows.
	char *record_in_table = NULL;
	get_table_records(tab_header, &record_in_table);
	record_row record_rows[MAX_NUM_ROW];
	memset(record_rows, '\0', sizeof(record_rows));
	record_row *p_current_row = NULL;
	int num_loaded_records = 0;
	int num_affected_records = 0;
	for (int i = 0; i < tab_header->num_records; i++) {
		// Fill all field values (not only displayed columns) from the current record.
		p_current_row = &record_rows[num_loaded_records];
		fill_record_row(cd_entries, tab_entry->num_columns, p_current_row,
			record_in_table);
		num_loaded_records++;

		// Update qualified records.
		if ((!has_where_clause) ||
			apply_row_predicate(cd_entries, tab_entry->num_columns, p_current_row,
				&row_filter)) {
			// Only update the record if the value is really changed.
			bool value_changed = false;
			if (value_to_update.type == INT_FIELD) {
				if (value_to_update.null_type) {
					// Update NULL integer value.
					if (!p_current_row->value_ptrs[value_to_update.col_id]->null_type) {
						p_current_row->value_ptrs[value_to_update.col_id]->null_type = true;
						p_current_row->value_ptrs[value_to_update.col_id]->int_field = 0;
						value_changed = true;
					}
				}
				else {
					// Update real integer value.
					if (p_current_row->value_ptrs[value_to_update.col_id]->null_type ||
						p_current_row->value_ptrs[value_to_update.col_id]->int_field !=
						value_to_update.int_field) {
						p_current_row->value_ptrs[value_to_update.col_id]->null_type = false;
						p_current_row->value_ptrs[value_to_update.col_id]->int_field =
							value_to_update.int_field;
						value_changed = true;
					}
				}
			}
			else {
				if (value_to_update.null_type) {
					// Update NULL string value.
					if (!p_current_row->value_ptrs[value_to_update.col_id]->null_type) {
						p_current_row->value_ptrs[value_to_update.col_id]->null_type = true;
						memset(
							p_current_row->value_ptrs[value_to_update.col_id]->string_field,
							'\0', MAX_STRING_LEN + 1);
						value_changed = true;
					}
				}
				else {
					// Update real string value.
					if (p_current_row->value_ptrs[value_to_update.col_id]->null_type ||
						strcmp(p_current_row->value_ptrs[value_to_update.col_id]
							->string_field,
							value_to_update.string_field) != 0) {
						p_current_row->value_ptrs[value_to_update.col_id]->null_type = false;
						strcpy(
							p_current_row->value_ptrs[value_to_update.col_id]->string_field,
							value_to_update.string_field);
						value_changed = true;
					}
				}
			}
			if (value_changed) {
				fill_raw_record_bytes(cd_entries, p_current_row->value_ptrs,
					tab_entry->num_columns, record_in_table,
					tab_header->record_len);
				num_affected_records++;
			}
		}

		// Move forward to the next record.
		record_in_table += tab_header->record_len;
	}

	printf("Affected records: %d\n", num_affected_records);

	if (num_affected_records > 0) {
		// Write records back to the .tab file.
		char table_filename[MAX_IDENT_LEN + 5];
		sprintf(table_filename, "%s.tab", tab_header->tpd_ptr->table_name);
		FILE *fhandle = NULL;
		if ((fhandle = fopen(table_filename, "wbc")) == NULL) {
			rc = FILE_OPEN_ERROR;
		}
		else {
			tab_header->tpd_ptr = NULL;  // Reset tpd pointer.
			fwrite(tab_header, tab_header->table_file_len, 1, fhandle);
			fflush(fhandle);
			fclose(fhandle);
		}
	}
	else {
		printf("[warning] No records were updated.\n");
	}

	free(tab_header);
	return rc;
}

int sem_delete(token_list *t_list) {
	int rc = 0;
	token_list *cur = t_list;

	// Check table name.
	if (!can_be_identifier(cur)) {
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
		return rc;
	}

	// Check whether the table name exists.
	tpd_entry *tab_entry = get_tpd_from_list(cur->tok_string);
	if (tab_entry == NULL) {
		rc = TABLE_NOT_EXIST;
		cur->tok_value = INVALID;
		return rc;
	}

	// Get column descriptors.
	cd_entry *cd_entries = NULL;
	get_cd_entries(tab_entry, &cd_entries);

	bool has_where_clause = false;
	record_predicate row_filter;
	memset(&row_filter, '\0', sizeof(row_filter));
	// Parse WHERE clause.
	cur = cur->next;
	if (cur->tok_value == K_WHERE) {
		has_where_clause = true;
		row_filter.type = K_AND;
		row_filter.num_conditions = 1;

		// Parse name of the filtering column.
		cur = cur->next;
		if (can_be_identifier(cur)) {
			int col_index = get_cd_entry_index(cd_entries, tab_entry->num_columns,
				cur->tok_string);
			if (col_index > -1) {
				row_filter.conditions[0].col_id = col_index;
				row_filter.conditions[0].value_type =
					((cd_entries[col_index].col_type == T_INT)
					? INT_FIELD
					: STRING_FIELD);
			} else {
				rc = INVALID_COLUMN_NAME;
				cur->tok_value = INVALID;
				return rc;
			}
		} else {
			rc = INVALID_COLUMN_NAME;
			cur->tok_value = INVALID;
			return rc;
		}

		// Parse the operator and operand of the condition.
		cur = cur->next;
		if (cur->tok_value == S_LESS || cur->tok_value == S_GREATER ||
			cur->tok_value == S_EQUAL) {
			row_filter.conditions[0].op_type = cur->tok_value;
			cur = cur->next;
			if (cur->tok_value == INT_LITERAL) {
				if (row_filter.conditions[0].value_type == INT_FIELD) {
					row_filter.conditions[0].int_data_value = atoi(cur->tok_string);
				} else {
					rc = INVALID_CONDITION_OPERAND;
					cur->tok_value = INVALID;
					return rc;
				}
			} else if (cur->tok_value == STRING_LITERAL) {
				if (row_filter.conditions[0].value_type == STRING_FIELD) {
					strcpy(row_filter.conditions[0].string_data_value, cur->tok_string);
				} else {
					rc = INVALID_CONDITION_OPERAND;
					cur->tok_value = INVALID;
					return rc;
				}
			} else {
				rc = INVALID_CONDITION;
				cur->tok_value = INVALID;
				return rc;
			}
		} else if (cur->tok_value == K_IS &&
			cur->next->tok_value == K_NULL) {  // "IS NULL"
			cur = cur->next;
			row_filter.conditions[0].op_type = K_IS;
		} else if (cur->tok_value == K_IS && cur->next->tok_value == K_NOT &&
			cur->next->next->tok_value == K_NULL) {  // "IS NOT NULL"
			cur = cur->next->next;
			row_filter.conditions[0].op_type = K_NOT;
		} else {
			rc = INVALID_CONDITION;
			cur->tok_value = INVALID;
			return rc;
		}
		cur = cur->next;
	}

	if (cur->tok_value != EOC) {
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
		return rc;
	}

	// It is the heap memory owner of the content of the whole table.
	// Do not forget to free it later.
	table_file *tab_header = NULL;
	if ((rc = load_table_records(tab_entry, &tab_header)) != 0) {
		return rc;
	}

	// Load record rows.
	char *record_in_table = NULL;
	get_table_records(tab_header, &record_in_table);
	record_row *p_first_row = NULL;
	record_row *p_current_row = NULL;
	record_row *p_previous_row = NULL;
	int num_loaded_records = 0;
	int num_affected_records = 0;
	for (int i = 0; i < tab_header->num_records; i++) {
		// Fill all field values (not only displayed columns) from current record.
		if (p_previous_row == NULL) {  // The first record will be loaded.
			p_current_row = (record_row *)malloc(sizeof(record_row));
			p_first_row = p_current_row;
		} else {
			p_current_row = (record_row *)malloc(sizeof(record_row));
			p_previous_row->next = p_current_row;
		}
		fill_record_row(cd_entries, tab_entry->num_columns, p_current_row,
			record_in_table);
		num_loaded_records++;

		// Delete qualified records.
		if ((!has_where_clause) ||
			apply_row_predicate(cd_entries, tab_entry->num_columns, p_current_row,
				&row_filter)) {
			free_record_row(p_current_row, false);
			p_current_row = NULL;
			if (p_previous_row == NULL) {
				p_first_row = NULL;
			} else {
				p_previous_row->next = NULL;
			}
			num_affected_records++;
		} else {
			p_previous_row = p_current_row;
		}

		// Move forward to the next record.
		record_in_table += tab_header->record_len;
	}

	printf("Affected records: %d\n", num_affected_records);
	if (num_affected_records > 0) {
		// Write records back to .tab file.
		rc = save_records_to_file(tab_header, p_first_row);
	} else {
		printf("[warning] No records were deleted.\n");
	}

	free_record_row(p_first_row, true);
	free(tab_header);
	return rc;
}


int initialize_tpd_list()
{
	int rc = 0;
	FILE *fhandle = NULL;
//	struct _stat file_stat;
	struct stat file_stat;

  /* Open for read */
  if((fhandle = fopen("dbfile.bin", "rbc")) == NULL)
	{
		if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
		{
			rc = FILE_OPEN_ERROR;
		}
    else
		{
			g_tpd_list = NULL;
			g_tpd_list = (tpd_list*)calloc(1, sizeof(tpd_list));
			
			if (!g_tpd_list)
			{
				rc = MEMORY_ERROR;
			}
			else
			{
				g_tpd_list->list_size = sizeof(tpd_list);
				fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
				fflush(fhandle);
				fclose(fhandle);
			}
		}
	}
	else
	{
		/* There is a valid dbfile.bin file - get file size */
//		fstat(fileno(fhandle), &file_stat);
		fstat(fileno(fhandle), &file_stat);
		printf("dbfile.bin size = %d\n", file_stat.st_size);

		g_tpd_list = (tpd_list*)calloc(1, file_stat.st_size);

		if (!g_tpd_list)
		{
			rc = MEMORY_ERROR;
		}
		else
		{
			fread(g_tpd_list, file_stat.st_size, 1, fhandle);
			fflush(fhandle);
			fclose(fhandle);

			if (g_tpd_list->list_size != file_stat.st_size)
			{
				rc = DBFILE_CORRUPTION;
			}

		}
	}
    
	return rc;
}
	
int add_tpd_to_list(tpd_entry *tpd)
{
	int rc = 0;
	int old_size = 0;
	FILE *fhandle = NULL;

	if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
	{
		rc = FILE_OPEN_ERROR;
	}
  else
	{
		old_size = g_tpd_list->list_size;

		if (g_tpd_list->num_tables == 0)
		{
			/* If this is an empty list, overlap the dummy header */
			g_tpd_list->num_tables++;
		 	g_tpd_list->list_size += (tpd->tpd_size - sizeof(tpd_entry));
			fwrite(g_tpd_list, old_size - sizeof(tpd_entry), 1, fhandle);
		}
		else
		{
			/* There is at least 1, just append at the end */
			g_tpd_list->num_tables++;
		 	g_tpd_list->list_size += tpd->tpd_size;
			fwrite(g_tpd_list, old_size, 1, fhandle);
		}

		fwrite(tpd, tpd->tpd_size, 1, fhandle);
		fflush(fhandle);
		fclose(fhandle);
	}

	return rc;
}

int drop_tpd_from_list(char *tabname)
{
	int rc = 0;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;
	int count = 0;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (strcasecmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				int old_size = 0;
				FILE *fhandle = NULL;

				if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
				{
					rc = FILE_OPEN_ERROR;
				}
			  else
				{
					old_size = g_tpd_list->list_size;

					if (count == 0)
					{
						/* If this is the first entry */
						g_tpd_list->num_tables--;

						if (g_tpd_list->num_tables == 0)
						{
							/* This is the last table, null out dummy header */
							memset((void*)g_tpd_list, '\0', sizeof(tpd_list));
							g_tpd_list->list_size = sizeof(tpd_list);
							fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
						}
						else
						{
							/* First in list, but not the last one */
							g_tpd_list->list_size -= cur->tpd_size;

							/* First, write the 8 byte header */
							fwrite(g_tpd_list, sizeof(tpd_list) - sizeof(tpd_entry),
								     1, fhandle);

							/* Now write everything starting after the cur entry */
							fwrite((char*)cur + cur->tpd_size,
								     old_size - cur->tpd_size -
										 (sizeof(tpd_list) - sizeof(tpd_entry)),
								     1, fhandle);
						}
					}
					else
					{
						/* This is NOT the first entry - count > 0 */
						g_tpd_list->num_tables--;
					 	g_tpd_list->list_size -= cur->tpd_size;

						/* First, write everything from beginning to cur */
						fwrite(g_tpd_list, ((char*)cur - (char*)g_tpd_list),
									 1, fhandle);

						/* Check if cur is the last entry. Note that g_tdp_list->list_size
						   has already subtracted the cur->tpd_size, therefore it will
						   point to the start of cur if cur was the last entry */
						if ((char*)g_tpd_list + g_tpd_list->list_size == (char*)cur)
						{
							/* If true, nothing else to write */
						}
						else
						{
							/* NOT the last entry, copy everything from the beginning of the
							   next entry which is (cur + cur->tpd_size) and the remaining size */
							fwrite((char*)cur + cur->tpd_size,
										 old_size - cur->tpd_size -
										 ((char*)cur - (char*)g_tpd_list),							     
								     1, fhandle);
						}
					}

					fflush(fhandle);
					fclose(fhandle);
				}

				
			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry*)((char*)cur + cur->tpd_size);
					count++;
				}
			}
		}
	}
	
	if (!found)
	{
		rc = INVALID_TABLE_NAME;
	}

	return rc;
}

tpd_entry* get_tpd_from_list(char *tabname)
{
	tpd_entry *tpd = NULL;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (strcasecmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				tpd = cur;
			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry*)((char*)cur + cur->tpd_size);
				}
			}
		}
	}

	return tpd;
}

int create_tab_file(char *table_name, cd_entry cd_entries[], int num_columns) {
  int rc = 0;
  table_file tab_header;

  int cur_record_size = 0;
  for (int i = 0; i < num_columns; i++) {
    cur_record_size += (1 + cd_entries[i].col_len);
  }
  // The total record_len must be rounded to a 4-byte boundary.
  cur_record_size = round_integer(cur_record_size, 4);

  tab_header.table_file_len = sizeof(table_file);
  tab_header.record_len = cur_record_size;
  tab_header.num_records = 0;
  tab_header.offset = sizeof(table_file);
  tab_header.tpd_ptr = NULL;  // Reset tpd pointer.

  char table_filename[MAX_IDENT_LEN + 5];
  sprintf(table_filename, "%s.tab", table_name);
  FILE *fhandle = NULL;

  if ((fhandle = fopen(table_filename, "wbc")) == NULL) {
    rc = FILE_OPEN_ERROR;
  } else {
    fwrite((void *)&tab_header, tab_header.table_file_len, 1, fhandle);
    fflush(fhandle);
    fclose(fhandle);
  }

  return rc;
}

int check_insert_values(field_val field_values[], int num_values,
                        cd_entry cd_entries[], int num_columns) {
  int rc = 0;
  if (num_values != num_columns) {
    return INVALID_VALUES_COUNT;
  }

  for (int i = 0; i < num_values; i++) {
    if (field_values[i].null_type) {
      // Field value is NULL.
      if (cd_entries[i].col_type == T_INT) {
        field_values[i].type = INT_FIELD;
      } else {
        field_values[i].type = STRING_FIELD;
      }
      if (cd_entries[i].not_null) {
        field_values[i].token->tok_value = INVALID;
        rc = UNEXPECTED_NULL_VALUE;
        break;
      }
    } else {
      // Field value is not NULL, so it must be either integer or string.
      if (field_values[i].type == INT_FIELD) {
        if (cd_entries[i].col_type != T_INT) {
          field_values[i].token->tok_value = INVALID;
          rc = DATA_TYPE_MISMATCH;
          break;
        }
      } else if (field_values[i].type == STRING_FIELD) {
        if ((cd_entries[i].col_type != T_CHAR) ||
            (cd_entries[i].col_len <
             (int)strlen(field_values[i].string_field))) {
          field_values[i].token->tok_value = INVALID;
          rc = DATA_TYPE_MISMATCH;
          break;
        }
      }
    }
  }
  return rc;
}

void free_token_list(token_list *const t_list) {
  token_list *tok_ptr = t_list;
  token_list *tmp_tok_ptr = NULL;
  while (tok_ptr != NULL) {
    tmp_tok_ptr = tok_ptr->next;
    free(tok_ptr);
    tok_ptr = tmp_tok_ptr;
  }
}

int load_table_records(tpd_entry *tpd, table_file **pp_table_header) {
  int rc = 0;
  char table_filename[MAX_IDENT_LEN + 5];
  sprintf(table_filename, "%s.tab", tpd->table_name);
  FILE *fhandle = NULL;
  if ((fhandle = fopen(table_filename, "rbc")) == NULL) {
    return FILE_OPEN_ERROR;
  }
  int cur_file_size = get_file_size(fhandle);
  table_file *tab_header = (table_file *)malloc(cur_file_size);
  fread(tab_header, cur_file_size, 1, fhandle);
  fclose(fhandle);
  if (tab_header->table_file_len != cur_file_size) {
    rc = TABFILE_CORRUPTION;
    free(tab_header);
  }
  tab_header->tpd_ptr = tpd;
  *pp_table_header = tab_header;
  return rc;
}

int get_file_size(FILE *fhandle) {
  if (!fhandle) {
    return -1;
  }
  struct stat file_stat;
  fstat(fileno(fhandle), &file_stat);
  return (int)(file_stat.st_size);
}

int fill_raw_record_bytes(cd_entry cd_entries[], field_val *field_values[],
                          int num_cols, char record_bytes[],
                          int num_record_bytes) {
  memset(record_bytes, '\0', num_record_bytes);
  unsigned char value_length = 0;
  int cur_offset_in_record = 0;
  int cur_int_value = 0;
  char *cur_string_value = NULL;
  for (int i = 0; i < num_cols; i++) {
    if (field_values[i]->type == INT_FIELD) {
      // Store a integer.
      if (field_values[i]->null_type) {
        // Null value.
        value_length = 0;
        memcpy(record_bytes + cur_offset_in_record, &value_length, 1);
        cur_offset_in_record += (1 + cd_entries[i].col_len);
      } else {
        // Integer value.
        cur_int_value = field_values[i]->int_field;
        value_length = cd_entries[i].col_len;
        memcpy(record_bytes + cur_offset_in_record, &value_length, 1);
        cur_offset_in_record += 1;
        memcpy(record_bytes + cur_offset_in_record, &cur_int_value, value_length);
        cur_offset_in_record += cd_entries[i].col_len;
      }
    } else {
      // Store a string.
      if (field_values[i]->null_type) {
        // Null value.
        value_length = 0;
        memcpy(record_bytes + cur_offset_in_record, &value_length, 1);
        cur_offset_in_record += (1 + cd_entries[i].col_len);
      } else {
        // String value.
        cur_string_value = field_values[i]->string_field;
        value_length = strlen(cur_string_value);
        memcpy(record_bytes + cur_offset_in_record, &value_length, 1);
        cur_offset_in_record += 1;
        memcpy(record_bytes + cur_offset_in_record, cur_string_value,
               strlen(cur_string_value));
        cur_offset_in_record += cd_entries[i].col_len;
      }
    }
  }
  return cur_offset_in_record;
}

int fill_record_row(cd_entry cd_entries[], int num_cols, record_row *p_row,
                    char record_bytes[]) {
  memset(p_row, '\0', sizeof(record_row));

  int offset_in_record = 0;
  unsigned char value_length = 0;
  field_val *p_field_value = NULL;
  for (int i = 0; i < num_cols; i++) {
    // Get value length.
    memcpy(&value_length, record_bytes + offset_in_record, 1);
    offset_in_record += 1;

    // Get field value.
    p_field_value = (field_val *)malloc(sizeof(field_val));
    p_field_value->col_id = cd_entries[i].col_id;
    p_field_value->null_type = (value_length == 0);
    p_field_value->token = NULL;
    if (cd_entries[i].col_type == T_INT) {
      // Set an integer.
      p_field_value->type = INT_FIELD;
      if (!p_field_value->null_type) {
        memcpy(&p_field_value->int_field, record_bytes + offset_in_record,
               value_length);
      }
    } else {
      // Set a string.
      p_field_value->type = STRING_FIELD;
      if (!p_field_value->null_type) {
        memcpy(p_field_value->string_field, record_bytes + offset_in_record,
               value_length);
        p_field_value->string_field[value_length] = '\0';
      }
    }
    p_row->value_ptrs[i] = p_field_value;
    offset_in_record += cd_entries[i].col_len;
  }
  p_row->num_fields = num_cols;
  p_row->sort_column = -1;
  p_row->next = NULL;
  return offset_in_record;
}

void print_table_border(cd_entry *sorted_cd_entries[], int num_values) {
  int col_width = 0;
  for (int i = 0; i < num_values; i++) {
    printf("+");
    col_width = column_display_width(sorted_cd_entries[i]);
    repeat_print_char('-', col_width + 2);
  }
  printf("+\n");
}

void print_table_column_names(cd_entry *sorted_cd_entries[],
                              record_field_name field_names[], int num_values) {
  int col_gap = 0;
  for (int i = 0; i < num_values; i++) {
    printf("%c %s", '|', field_names[i].name);
    col_gap = column_display_width(sorted_cd_entries[i]) -
              strlen(sorted_cd_entries[i]->col_name) + 1;
    repeat_print_char(' ', col_gap);
  }
  printf("%c\n", '|');
}

int column_display_width(cd_entry *col_entry) {
  int col_name_len = strlen(col_entry->col_name);
  if (col_entry->col_len > col_name_len) {
    return col_entry->col_len;
  } else {
    return col_name_len;
  }
}

void print_record_row(cd_entry *sorted_cd_entries[], int num_cols,
                      record_row *row) {
  int col_gap = 0;
  char display_value[MAX_STRING_LEN + 1];
  int col_index = -1;
  field_val **field_values = row->value_ptrs;
  bool left_align = true;
  for (int i = 0; i < num_cols; i++) {
    col_index = sorted_cd_entries[i]->col_id;
    left_align = true;
    if (!field_values[col_index]->null_type) {
      if (field_values[col_index]->type == INT_FIELD) {
        left_align = false;
        sprintf(display_value, "%d", field_values[col_index]->int_field);
      } else {
        strcpy(display_value, field_values[col_index]->string_field);
      }
    } else {
      // Display NULL value as a dash.
      strcpy(display_value, "-");
      left_align = (field_values[col_index]->type == STRING_FIELD);
    }
    col_gap =
        column_display_width(sorted_cd_entries[i]) - strlen(display_value) + 1;
    if (left_align) {
      printf("| %s", display_value);
      repeat_print_char(' ', col_gap);
    } else {
      printf("|");
      repeat_print_char(' ', col_gap);
      printf("%s ", display_value);
    }
  }
  printf("%c\n", '|');
}

int get_cd_entry_index(cd_entry cd_entries[], int num_cols, char *col_name) {
  for (int i = 0; i < num_cols; i++) {
    // Column names are case-insensitive.
    if (strcmp(cd_entries[i].col_name, col_name) == 0) {
      return i;
    }
  }
  return -1;
}

void print_aggregate_result(int aggregate_type, int num_fields,
                            int records_count, int int_sum,
                            cd_entry *sorted_cd_entries[]) {
  char display_value[MAX_STRING_LEN + 1];
  memset(display_value, '\0', sizeof(display_value));
  if (aggregate_type == F_SUM) {
    sprintf(display_value, "%d", int_sum);
  } else if (aggregate_type == F_AVG) {
    if (records_count == 0) {
      // Divided by zero error, show as NaN (i.e. Not-a-number).
      sprintf(display_value, "NaN");
    } else {
      sprintf(display_value, "%d", int_sum / records_count);
    }
  } else if (aggregate_type == F_COUNT) {
    sprintf(display_value, "%d", records_count);
  }

  char display_title[MAX_STRING_LEN + 1];
  memset(display_title, '\0', sizeof(display_title));
  if (num_fields == 1) {
    // Aggregate on one column.
    if (aggregate_type == F_SUM) {
      sprintf(display_title, "SUM(%s)", sorted_cd_entries[0]->col_name);
    } else if (aggregate_type == F_AVG) {
      sprintf(display_title, "AVG(%s)", sorted_cd_entries[0]->col_name);
    } else {  // F_COUNT
      sprintf(display_title, "COUNT(%s)", sorted_cd_entries[0]->col_name);
    }
  } else {
    // Aggregate on one column.
    sprintf(display_title, "COUNT(*)");
  }

  int display_width = (strlen(display_value) > strlen(display_title))
                          ? strlen(display_value)
                          : strlen(display_title);
  printf("+");
  repeat_print_char('-', display_width + 2);
  printf("+\n");

  printf("| %s ", display_title);
  repeat_print_char(' ', display_width - strlen(display_title));
  printf("|\n");

  printf("+");
  repeat_print_char('-', display_width + 2);
  printf("+\n");

  printf("| %s ", display_value);
  repeat_print_char(' ', display_width - strlen(display_value));
  printf("|\n");

  printf("+");
  repeat_print_char('-', display_width + 2);
  printf("+\n");
}

bool apply_row_predicate(cd_entry cd_entries[], int num_cols, record_row *p_row,
                         record_predicate *p_predicate) {
  if ((!p_predicate) || (p_predicate->num_conditions < 1)) {
    return true;
  }

  // Evaluate the first condition.
  field_val *lhs_operand =
      p_row->value_ptrs[p_predicate->conditions[0].col_id];
  bool result = eval_condition(&p_predicate->conditions[0], lhs_operand);
  if (p_predicate->num_conditions == 1) {
    // Only one condition.
    return result;
  }

  // Evaluate the second condition.
  lhs_operand = p_row->value_ptrs[p_predicate->conditions[1].col_id];
  if (p_predicate->type == K_AND) {
    // AND conditions.
    result = result && eval_condition(&p_predicate->conditions[1], lhs_operand);
  } else {
    // OR conditions.
    result = result || eval_condition(&p_predicate->conditions[1], lhs_operand);
  }

  return result;
}

bool eval_condition(record_condition *p_condition, field_val *p_field_value) {
  bool result = true;
  switch (p_condition->op_type) {
    case S_LESS:
      // Operator "<"
      if (p_condition->value_type == INT_FIELD) {
        if (p_field_value->null_type) {
          result = false;
        } else {
          result = (p_field_value->int_field < p_condition->int_data_value);
        }
      } else {
        if (p_field_value->null_type) {
          result = false;
        } else {
          result = (strcmp(p_field_value->string_field,
                           p_condition->string_data_value) < 0);
        }
      }
      break;
    case S_EQUAL:
      // Operator "="
      if (p_condition->value_type == INT_FIELD) {
        if (p_field_value->null_type) {
          result = false;
        } else {
          result = (p_field_value->int_field == p_condition->int_data_value);
        }
      } else {
        if (p_field_value->null_type) {
          result = false;
        } else {
          result = (strcmp(p_field_value->string_field,
                           p_condition->string_data_value) == 0);
        }
      }
      break;
    case S_GREATER:
      // Operator ">"
      if (p_condition->value_type == INT_FIELD) {
        if (p_field_value->null_type) {
          result = false;
        } else {
          result = (p_field_value->int_field > p_condition->int_data_value);
        }
      } else {
        if (p_field_value->null_type) {
          result = false;
        } else {
          result = (strcmp(p_field_value->string_field,
                           p_condition->string_data_value) > 0);
        }
      }
      break;
    case K_IS:
      // Operator "IS_NULL"
      result = p_field_value->null_type;
      break;
    case K_NOT:
      // Operator "IS_NOT_NULL"
      result = !p_field_value->null_type;
      break;
    default:
      // Return true for unknown relational operators.
      printf("[warning] unknown relational operator: %d\n",
             p_condition->op_type);
      result = true;
  }
  return result;
}

void sort_records(record_row rows[], int num_records, cd_entry *p_sorting_col,
                  bool is_desc) {
  for (int i = 0; i < num_records; i++) {
    rows[i].sort_column = p_sorting_col->col_id;
  }
  qsort(rows, num_records, sizeof(record_row), records_comparator);
  record_row temp_record;
  if (is_desc) {
    for (int i = 0; i < num_records / 2; i++) {
      // temp_record := rows[i];
      memcpy(&temp_record, &rows[i], sizeof(record_row));
      // rows[i] := rows[num_records - 1 - i];
      memcpy(&rows[i], &rows[num_records - 1 - i], sizeof(record_row));
      // rows[num_records - 1 - i] := temp_record;
      memcpy(&rows[num_records - 1 - i], &temp_record, sizeof(record_row));
    }
  }
}

int records_comparator(const void *arg1, const void *arg2) {
  record_row *p_record1 = (record_row *)arg1;
  record_row *p_record2 = (record_row *)arg2;
  int sort_column = p_record1->sort_column;

  // If result < 0, elem1 less than elem2;
  // If result = 0, elem1 equivalent to elem2;
  // If result > 0, elem1 greater than elem2.
  int result = 0;
  field_val *p_value1 = p_record1->value_ptrs[sort_column];
  field_val *p_value2 = p_record2->value_ptrs[sort_column];
  if (p_value1->type == INT_FIELD) {
    // Compare 2 integers.
    // NULL is smaller than any other values.
    if (p_value1->null_type) {
      result = (p_value2->null_type ? 0 : -1);
    } else {
      if (p_value2->null_type) {
        result = 1;
      } else if (p_value1->int_field < p_value2->int_field) {
        result = -1;
      } else if (p_value1->int_field > p_value2->int_field) {
        result = 1;
      } else {
        result = 0;
      }
    }
  } else {
    // Compare 2 strings.
    // NULL is smaller than any other values.
    if (p_value1->null_type) {
      result = (p_value2->null_type ? 0 : -1);
    } else {
      if (p_value2->null_type) {
        result = 1;
      } else {
        result = strcmp(p_value1->string_field, p_value2->string_field);
      }
    }
  }
  return result;
}

void free_record_row(record_row *row, bool to_last) {
  record_row *current_row = row;
  record_row *temp = NULL;
  while (current_row) {
    for (int i = 0; i < current_row->num_fields; i++) {
      free(current_row->value_ptrs[i]);
    }
    if (!to_last) {
      return;
    }
    temp = current_row;
    current_row = current_row->next;
    free(temp);
  }
}

int save_records_to_file(table_file *const tab_header,
                         record_row *const rows_head) {
  int rc = 0;
  tpd_entry *const tab_entry = tab_header->tpd_ptr;

  int num_rows = 0;
  record_row *current_row = rows_head;
  while (current_row) {
    num_rows++;
    current_row = current_row->next;
  }

  tab_header->num_records = num_rows;
  tab_header->table_file_len =
      sizeof(table_file) + tab_header->record_len * num_rows;
  tab_header->tpd_ptr = NULL;  // Reset tpd pointer.

  char table_filename[MAX_IDENT_LEN + 5];
  sprintf(table_filename, "%s.tab", tab_entry->table_name);
  FILE *fhandle = NULL;
  if ((fhandle = fopen(table_filename, "wbc")) == NULL) {
    rc = FILE_OPEN_ERROR;
  } else {
    fwrite(tab_header, tab_header->offset, 1, fhandle);

    char *record_raw_bytes = (char *)malloc(tab_header->record_len);
    cd_entry *cd_entries = NULL;
    get_cd_entries(tab_entry, &cd_entries);
    current_row = rows_head;
    while (current_row) {
      memset(record_raw_bytes, '\0', tab_header->record_len);
      fill_raw_record_bytes(cd_entries, current_row->value_ptrs,
                            tab_entry->num_columns, record_raw_bytes,
                            tab_header->record_len);
      fwrite(record_raw_bytes, tab_header->record_len, 1, fhandle);
      current_row = current_row->next;
    }
    free(record_raw_bytes);
    fflush(fhandle);
    fclose(fhandle);
  }

  return rc;
}

// int append_log_with_timestamp(const char *msg, time_t timestamp) {
//   char timestamp_text[MAX_LOG_ENTRY_TEXT_LEN + 1];
//   // Convert time to struct tm form in local time.
//   struct tm *t = localtime(&timestamp);
//   // Format is: yyyymmddhhmmss "SQL".
//   sprintf(timestamp_text, "%d%02d%02d%02d%02d%02d \"%s\"", 1900 + t->tm_year,
//           1 + t->tm_mon, t->tm_mday, t->tm_hour, t->tm_min, t->tm_sec, msg);
//   return write_log(timestamp_text, true);
// }

// int write_log(const char *msg, bool is_append) {
//   FILE *fhandle = fopen(kDbLogFile, is_append ? "a" : "w");
//   if (!fhandle) {
//     return FILE_OPEN_ERROR;
//   }
//   fprintf(fhandle, "%s\n", msg);
//   fclose(fhandle);
//   return 0;
// }

// int scan_log(log_entry **pp_first_log_entry) {
//   FILE *fhandle = fopen(kDbLogFile, "r");
//   if (!fhandle) {
//     return FILE_OPEN_ERROR;
//   }
//   log_entry *p_head = NULL;
//   log_entry *p_cur = NULL;
//   char log_text[MAX_LOG_ENTRY_TEXT_LEN + 1];
//   while (fgets(log_text, sizeof(log_text), fhandle)) {
//     if (!(strlen(log_text) > 1)) {  // skip empty line
//       continue;
//     }
//     if (p_cur == NULL) {  // first log entry
//       p_cur = (log_entry *)malloc(sizeof(log_entry));
//       p_head = p_cur;
//     } else {
//       p_cur->next = (log_entry *)malloc(sizeof(log_entry));
//       p_cur = p_cur->next;
//     }
//     if (is_a_sql_statement_log_entry(
//             log_text)) {  // a DDL/DML statement with datetime
//       memcpy(p_cur->datetime, log_text, LOG_ENTRY_TIMESTAMP_LEN);
//       p_cur->datetime[LOG_ENTRY_TIMESTAMP_LEN] = '\0';
//       strcpy(p_cur->text, log_text + LOG_ENTRY_TIMESTAMP_LEN + 2);
//     } else {  // a command without datetime
//       p_cur->datetime[0] = '\0';
//       strcpy(p_cur->text, log_text);
//     }
//     // Remove trailing double-quote and line-feed.
//     for (int i = strlen(p_cur->text) - 1;
//          (p_cur->text[i] == '\n' || p_cur->text[i] == '"'); i--) {
//       p_cur->text[i] = '\0';
//     }
//     strcpy(p_cur->raw_text, log_text);
//     // Remove trailing line-feed for raw_text.
//     int l = strlen(p_cur->raw_text) - 1;
//     if (p_cur->raw_text[l] == '\n') {
//       p_cur->raw_text[l] = '\0';
//     }
//     p_cur->next = NULL;
//   }
//   fclose(fhandle);
//   *pp_first_log_entry = p_head;
//   return 0;
// }
// 
// void free_log_entries(log_entry *p_first_log_entry) {
//   if (!p_first_log_entry) {
//     return;
//   }
//   log_entry *p = p_first_log_entry;
//   log_entry *temp = NULL;
//   while (p) {
//     temp = p;
//     p = p->next;
//     free(temp);
//   }
// }

// int restore_from_backup_file(char *backup_filename, int db_flags) {
//   // Reconstruct tpd_list and fetch table names.
//   FILE *f_backup = fopen(backup_filename, "rb");
//   FILE *f_tmp_dbfile = fopen(kTempDbFile, "wbc");
//   if (f_backup == NULL || f_tmp_dbfile == NULL) {
//     if (f_backup) {
//       fclose(f_backup);
//     }
//     if (f_tmp_dbfile) {
//       fclose(f_tmp_dbfile);
//     }
//     return FILE_OPEN_ERROR;
//   }
//   // This tpd_list will NOT hold the whole db metadata.
//   // It is only used to determine the real size of the whole db metadata.
//   tpd_list tmp_tpd_list;
//   fread(&tmp_tpd_list, sizeof(tmp_tpd_list), 1, f_backup);
//   // Set db_flags.
//   tmp_tpd_list.db_flags = db_flags;
//   // Backup db file with db_flag.
//   fwrite(&tmp_tpd_list, sizeof(tmp_tpd_list), 1, f_tmp_dbfile);
//   // Backup remaining bytes of the db file.
//   char byte;
//   for (int i = sizeof(tmp_tpd_list); i < tmp_tpd_list.list_size; i++) {
//     fread(&byte, sizeof(byte), 1, f_backup);
//     fwrite(&byte, sizeof(byte), 1, f_tmp_dbfile);
//   }
//   fflush(f_tmp_dbfile);
//   fclose(f_tmp_dbfile);
//   tpd_list *p_tpd_list = NULL;
//   initialize_tpd_list(kTempDbFile, &p_tpd_list);

//   // Backup .tab file for each table.
//   tpd_entry *cur_entry = &(p_tpd_list->tpd_start);
//   for (int i = 0; i < p_tpd_list->num_tables; i++) {
//     // Get table file size.
//     int32_t table_file_size;
//     fread(&table_file_size, sizeof(table_file_size), 1, f_backup);
//     // Writing table file as a .tab.temp file.
//     char table_filename[MAX_IDENT_LEN + 10];
//     sprintf(table_filename, "%s.tab.temp", cur_entry->table_name);
//     FILE *f_tmp_tab_file = fopen(table_filename, "wbc");
//     if (f_tmp_tab_file == NULL) {
//       return FILE_OPEN_ERROR;
//     }
//     for (int j = 0; j < table_file_size; j++) {
//       fread(&byte, sizeof(byte), 1, f_backup);
//       fwrite(&byte, sizeof(byte), 1, f_tmp_tab_file);
//     }
//     fclose(f_tmp_tab_file);

//     cur_entry =
//         (tpd_entry *)((char *)cur_entry + cur_entry->tpd_size);  // next table
//   }
//   fclose(f_backup);

//   // Remove old dbfile and .tab files.
//   remove(kDbFile);
//   list_tables(g_tpd_list, remove_table_file);

//   // Rename ".temp" suffix for all reconstructed files.
//   rename(kTempDbFile, kDbFile);
//   list_tables(p_tpd_list, rename_table_file);

//   // Refresh g_tpd_list.
//   free(g_tpd_list);
//   g_tpd_list = p_tpd_list;
//   return 0;
// }

void remove_table_file(tpd_entry *table_entry) {
  char filename[MAX_IDENT_LEN + 5];
  sprintf(filename, "%s.tab", table_entry->table_name);
  remove(filename);
}

void rename_table_file(tpd_entry *table_entry) {
  char src_filename[MAX_IDENT_LEN + 10];
  sprintf(src_filename, "%s.tab.temp", table_entry->table_name);
  char dst_filename[MAX_IDENT_LEN + 5];
  sprintf(dst_filename, "%s.tab", table_entry->table_name);
  rename(src_filename, dst_filename);
}

int list_tables(tpd_list *table_entries,
                void (*callback)(tpd_entry *table_entry)) {
  int num_tables = table_entries->num_tables;
  if (num_tables > 0) {
    tpd_entry *cur_entry = &(table_entries->tpd_start);
    for (int i = 0; i < num_tables; i++) {
      if (callback != NULL) {
        callback(cur_entry);
      }
      cur_entry = (tpd_entry *)((char *)cur_entry + cur_entry->tpd_size);
    }
  }
  return num_tables;
}

// int update_db_flags(int db_flags) {
//   FILE *fhandle = fopen(kDbFile, "r+b");
//   if (fhandle == NULL) {
//     return FILE_OPEN_ERROR;
//   }
//   tpd_list tmp_tpd_list;
//   fread(&tmp_tpd_list, sizeof(tmp_tpd_list), 1, fhandle);
//   tmp_tpd_list.db_flags = db_flags;
//   rewind(fhandle);
//   fwrite(&tmp_tpd_list, sizeof(tmp_tpd_list), 1, fhandle);
//   fclose(fhandle);
//   g_tpd_list->db_flags = db_flags;
//   return 0;
// }

// int backup_log_file(log_entry *log_entry_head) {
//   char log_bak_filename[10];
//   for (int i = 1; i <= MAX_NUM_LOG_BACKUP_FILES; i++) {
//     sprintf(log_bak_filename, "%s%d", kDbLogFile, i);
//     if (!is_file_readable(log_bak_filename, true)) {
//       break;
//     }
//   }
//   // We found one available log backup filename.
//   FILE *f_log_bak = fopen(log_bak_filename, "w");
//   if (f_log_bak == NULL) {
//     return FILE_OPEN_ERROR;
//   }
//   log_entry *cur_entry = log_entry_head;
//   while (cur_entry) {
//     if (stricmp(cur_entry->text, kRfStartLogEntry) != 0) {
//       fprintf(f_log_bak, "%s\n", cur_entry->raw_text);
//     }
//     cur_entry = cur_entry->next;
//   }
//   fclose(f_log_bak);
//   return 0;
// }

int max_days_of_month(int year, int month) {
  int days_in_month[] = {-1, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  if (year % 4 == 0 && year % 100 != 0) {
    days_in_month[2] = 29;
  } else if (year % 400 == 0) {
    days_in_month[2] = 29;
  }
  return days_in_month[month];
}

bool is_timestamp_valid(char *text) {
  // A valid timestamp should contains 14 digits, like 20140511093524.
  int num_digits = 0;
  char *p = text;
  while (*p) {
    if (!isdigit(*p)) {
      return false;
    }
    num_digits++;
    p++;
  }
  if (num_digits != 14) {
    return false;
  }

  char buffer[5];
  // Parse 4-digit year.
  memcpy(buffer, text, 4);
  buffer[4] = '\0';
  int year = atoi(buffer);
  if (year < 1900 || year > 2999) {
    return false;
  }

  // Parse 2-digit month.
  memcpy(buffer, text + 4, 2);
  buffer[2] = '\0';
  int month = atoi(buffer);
  if (month < 1 || month > 12) {
    return false;
  }

  // Parse 2-digit day.
  memcpy(buffer, text + 6, 2);
  buffer[2] = '\0';
  int day = atoi(buffer);
  if (day < 1 || day > max_days_of_month(year, month)) {
    return false;
  }

  // Parse 2-digit 24-hr hour, from 00 to 23.
  memcpy(buffer, text + 8, 2);
  buffer[2] = '\0';
  int hour = atoi(buffer);
  if (hour < 0 || hour > 23) {
    return false;
  }

  // Parse 2-digit minute.
  memcpy(buffer, text + 10, 2);
  buffer[2] = '\0';
  int minute = atoi(buffer);
  if (minute < 0 || minute > 59) {
    return false;
  }

  // Parse 2-digit second.
  memcpy(buffer, text + 12, 2);
  buffer[2] = '\0';
  int second = atoi(buffer);
  if (second < 0 || second > 59) {
    return false;
  }

  return true;
}
