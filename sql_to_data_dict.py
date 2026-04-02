"""
SQL Data Dictionary Extractor

Reads a .sql file containing multiple statements (including session
initialization), identifies SELECT statements, parses each one, and
writes a CSV data dictionary.

Usage:
    python sql_to_data_dict.py input.sql [output.csv]
"""

import sys
import csv
import re
import os

try:
    import sqlparse
except ImportError:
    print("Error: sqlparse is required. Run: pip install sqlparse")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CSV_HEADERS = [
    "Output Column Name",
    "Original Request Field Name",
    "Short Description",
    "Long Description",
    "Key / Possible Values",
    "Derived",
    "Base Field(s)",
    "Source Table(s)",
    "Logic",
]

# First keywords that identify session/init statements (not data queries)
SESSION_KEYWORDS = frozenset({
    "SET", "USE", "DECLARE", "ALTER", "GO", "EXEC", "EXECUTE",
    "DROP", "TRUNCATE", "GRANT", "REVOKE", "BEGIN", "COMMIT",
    "ROLLBACK", "CREATE", "INSERT", "UPDATE", "DELETE", "PRINT",
    "RAISERROR", "THROW", "IF", "WHILE", "RETURN",
})

# SQL keywords to exclude when extracting base field names
SQL_KEYWORDS = frozenset({
    "CASE", "WHEN", "THEN", "ELSE", "END", "AND", "OR", "NOT", "NULL",
    "AS", "IN", "IS", "BETWEEN", "LIKE", "TRUE", "FALSE",
    "SELECT", "FROM", "WHERE", "JOIN", "LEFT", "RIGHT", "INNER",
    "OUTER", "FULL", "CROSS", "ON", "GROUP", "BY", "ORDER", "HAVING",
    "OVER", "PARTITION", "ROWS", "RANGE", "DISTINCT", "TOP", "LIMIT",
    "ALL", "UNION", "EXCEPT", "INTERSECT", "WITH", "RECURSIVE",
    "CAST", "CONVERT", "COALESCE", "NULLIF", "IIF", "DECODE",
    "COUNT", "SUM", "AVG", "MIN", "MAX", "ROW_NUMBER", "RANK",
    "DENSE_RANK", "LEAD", "LAG", "FIRST_VALUE", "LAST_VALUE", "NTILE",
    "GETDATE", "NOW", "CURRENT_DATE", "CURRENT_TIMESTAMP", "TODAY",
    "DATEPART", "DATEDIFF", "DATEADD", "YEAR", "MONTH", "DAY",
    "SUBSTRING", "SUBSTR", "LEN", "LENGTH", "TRIM", "UPPER", "LOWER",
    "REPLACE", "CONCAT", "CATX", "DATE", "TIME",
    "NOCOUNT", "OFF", "ON", "ASC", "DESC", "NULLS", "FIRST", "LAST",
    "PRECEDING", "FOLLOWING", "UNBOUNDED", "CURRENT", "ROW",
    "YES", "NO", "UNKNOWN", "ACTIVE", "CLOSED", "HIGH", "MEDIUM", "LOW",
})

# Patterns for detecting derived expressions
CASE_PATTERN       = re.compile(r'\bCASE\b',               re.IGNORECASE)
FUNCTION_PATTERN   = re.compile(r'\b\w+\s*\(',             re.IGNORECASE)
ARITHMETIC_PATTERN = re.compile(r'(?<![<>!=])[+\-*/](?![>=])')
CONCAT_PATTERN     = re.compile(r'\|\|')

# Pattern for table references in FROM / JOIN
TABLE_REF_PATTERN = re.compile(
    r'(?:FROM|JOIN)\s+'
    r'((?:[a-zA-Z_][\w]*\.){0,3}[a-zA-Z_][\w]*)'   # table name (up to 3 parts)
    r'(?:\s+(?:AS\s+)?([a-zA-Z_][\w]*))?',           # optional alias
    re.IGNORECASE
)

# Alias at end of expression: ... AS alias_name
ALIAS_PATTERN = re.compile(r'\bAS\s+([a-zA-Z_"\'`][\w"\'`\s]*)\s*$', re.IGNORECASE)

# Field reference: optional_alias.field  or  just field
FIELD_REF_PATTERN = re.compile(r'\b(?:[a-zA-Z_]\w*\.)?([a-zA-Z_]\w*)\b')

# THEN / ELSE values in a CASE expression
THEN_PATTERN = re.compile(
    r"\bTHEN\s+('(?:[^'\\]|\\.)*'|[^\s,)]+)",
    re.IGNORECASE
)
ELSE_PATTERN = re.compile(
    r"\bELSE\s+('(?:[^'\\]|\\.)*'|[^\s,)]+)",
    re.IGNORECASE
)

# Inline and block comments
INLINE_COMMENT = re.compile(r'--[^\n]*')
BLOCK_COMMENT  = re.compile(r'/\*.*?\*/', re.DOTALL)

# String literals (single-quoted)
STRING_LITERAL = re.compile(r"'(?:[^'\\]|\\.)*'")


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def strip_comments(sql: str) -> str:
    """Remove -- and /* */ comments from SQL text."""
    sql = BLOCK_COMMENT.sub(' ', sql)
    sql = INLINE_COMMENT.sub(' ', sql)
    return sql


def strip_literals(sql: str) -> str:
    """Replace string literals with a placeholder so keywords inside
    string values don't affect parsing."""
    return STRING_LITERAL.sub("''", sql)


def normalize(sql: str) -> str:
    """Collapse whitespace and normalize to single spaces."""
    return re.sub(r'\s+', ' ', sql).strip()


def first_keyword(stmt_text: str) -> str:
    """Return the first non-comment, non-whitespace word of a statement."""
    cleaned = strip_comments(stmt_text).strip()
    match = re.match(r'([a-zA-Z_]\w*)', cleaned)
    return match.group(1).upper() if match else ''


# ---------------------------------------------------------------------------
# Statement filtering
# ---------------------------------------------------------------------------

def is_session_init(stmt_text: str) -> bool:
    """Return True if the statement is session initialization, not a query."""
    cleaned = strip_comments(stmt_text).strip()
    if not cleaned:
        return True  # empty / comments-only

    kw = first_keyword(cleaned)
    if kw in SESSION_KEYWORDS:
        # CREATE TABLE AS SELECT is a real query — keep it
        if kw == 'CREATE' and re.search(r'\bSELECT\b', cleaned, re.IGNORECASE):
            return False
        return True

    return False


def contains_select(stmt_text: str) -> bool:
    """Return True if the statement contains a SELECT clause."""
    cleaned = strip_comments(stmt_text)
    return bool(re.search(r'\bSELECT\b', cleaned, re.IGNORECASE))


# ---------------------------------------------------------------------------
# UNION ALL splitting
# ---------------------------------------------------------------------------

def split_on_union(sql: str) -> list:
    """Split a SQL string on UNION / UNION ALL at the top level (depth 0)."""
    parts = []
    depth = 0
    i = 0
    start = 0
    upper = sql.upper()

    while i < len(sql):
        ch = sql[i]
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
        elif depth == 0:
            # Check for UNION ALL or UNION
            for keyword in ('UNION ALL', 'UNION'):
                klen = len(keyword)
                if upper[i:i + klen] == keyword:
                    # Make sure it's a word boundary after
                    after = upper[i + klen: i + klen + 1]
                    if not after or not after.isalnum():
                        parts.append(sql[start:i].strip())
                        i += klen
                        start = i
                        break
            else:
                i += 1
            continue
        i += 1

    parts.append(sql[start:].strip())
    return [p for p in parts if p]


# ---------------------------------------------------------------------------
# Table reference extraction
# ---------------------------------------------------------------------------

def extract_table_references(sql: str) -> dict:
    """
    Return a dict mapping alias (upper) -> full table name
    for all FROM / JOIN references in the SQL text.
    """
    cleaned = strip_comments(sql)
    tables = {}

    skip_aliases = SESSION_KEYWORDS | {
        'WHERE', 'ON', 'AND', 'OR', 'SET', 'WHEN', 'THEN', 'ELSE',
        'END', 'BY', 'HAVING', 'UNION', 'ALL', 'SELECT', 'WITH',
    }

    for match in TABLE_REF_PATTERN.finditer(cleaned):
        full_name = match.group(1)
        alias     = match.group(2)

        if alias and alias.upper() in skip_aliases:
            alias = None

        key = (alias or full_name.split('.')[-1]).upper()
        tables[key] = full_name

        # Also register the last part of the name (table name without schema)
        short = full_name.split('.')[-1].upper()
        if short not in tables:
            tables[short] = full_name

        # Register full name as its own key
        tables[full_name.upper()] = full_name

    return tables


# ---------------------------------------------------------------------------
# Column list splitting
# ---------------------------------------------------------------------------

def split_column_list(select_clause: str) -> list:
    """
    Split a SELECT column list by commas, respecting nested parentheses.
    Returns a list of individual column expression strings.
    """
    columns = []
    depth = 0
    current = []

    for ch in select_clause:
        if ch == '(':
            depth += 1
            current.append(ch)
        elif ch == ')':
            depth -= 1
            current.append(ch)
        elif ch == ',' and depth == 0:
            col = ''.join(current).strip()
            if col:
                columns.append(col)
            current = []
        else:
            current.append(ch)

    col = ''.join(current).strip()
    if col:
        columns.append(col)

    return columns


def get_select_clause(sql: str) -> str:
    """
    Extract the text between SELECT and the first top-level FROM.
    Returns empty string if not found.
    """
    # Remove DISTINCT / TOP N / ALL modifiers right after SELECT
    sql_clean = strip_comments(sql)

    # Find top-level SELECT keyword
    sel_match = re.search(r'\bSELECT\b', sql_clean, re.IGNORECASE)
    if not sel_match:
        return ''

    start = sel_match.end()

    # Remove DISTINCT / ALL / TOP n
    remainder = sql_clean[start:]
    remainder = re.sub(r'^\s*(DISTINCT|ALL)\s+', '', remainder, flags=re.IGNORECASE)
    remainder = re.sub(r'^\s*TOP\s+\d+\s*', '', remainder, flags=re.IGNORECASE)

    # Find FROM at depth 0
    depth = 0
    i = 0
    upper = remainder.upper()
    while i < len(remainder):
        ch = remainder[i]
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
        elif depth == 0 and upper[i:i + 4] == 'FROM':
            # Check word boundary
            before = remainder[i - 1] if i > 0 else ' '
            after  = remainder[i + 4] if i + 4 < len(remainder) else ' '
            if not before.isalnum() and before != '_' and not after.isalnum() and after != '_':
                return remainder[:i].strip()
        i += 1

    return remainder.strip()


# ---------------------------------------------------------------------------
# Expression analysis
# ---------------------------------------------------------------------------

def detect_derived(expr: str) -> bool:
    """Return True if the expression uses functions, CASE, or arithmetic."""
    # Strip string literals so we don't match keywords inside them
    clean = strip_literals(expr)

    if CASE_PATTERN.search(clean):
        return True
    if CONCAT_PATTERN.search(clean):
        return True

    # Function call: word followed by '('
    # Exclude bare column references like "table.column"
    if FUNCTION_PATTERN.search(clean):
        return True

    # Arithmetic operators not part of comparison operators
    if ARITHMETIC_PATTERN.search(clean):
        return True

    return False


def extract_alias(expr: str) -> tuple:
    """
    Return (alias, raw_expr_without_alias).
    If no alias, alias is derived from the expression itself.
    """
    expr = expr.strip()

    # Find AS alias at the top level (not inside parentheses)
    depth = 0
    i = len(expr) - 1
    alias_start = -1

    # Walk backwards to find the last top-level AS
    tokens = re.split(r'\s+', expr)
    # Rebuild token positions
    pos = len(expr)
    for tok in reversed(tokens):
        pos -= len(tok)
        while pos > 0 and expr[pos - 1] == ' ':
            pos -= 1

        # Check parenthesis depth at this position
        depth = expr[:pos].count('(') - expr[:pos].count(')')
        if depth == 0 and tok.upper() not in ('AS',):
            # Check if previous token is AS
            before = expr[:pos].rstrip()
            if re.search(r'\bAS\s*$', before, re.IGNORECASE):
                alias = tok.strip('"\'`')
                raw   = re.sub(r'\s+AS\s+' + re.escape(tok) + r'\s*$', '',
                               expr, flags=re.IGNORECASE).strip()
                return alias.upper(), raw

    # Fallback: use regex
    m = ALIAS_PATTERN.search(expr)
    if m:
        alias = m.group(1).strip().strip('"\'`').upper()
        raw   = expr[:m.start()].strip()
        return alias, raw

    # No alias — use the field name from the expression
    # If it's table.field, take the field part
    simple = re.search(r'(?:[a-zA-Z_]\w*\.)?([a-zA-Z_]\w*)\s*$', expr)
    if simple:
        return simple.group(1).upper(), expr

    return expr.upper(), expr


def extract_base_fields(expr: str) -> list:
    """Return deduplicated list of base field names (no table alias prefix)."""
    clean = strip_literals(expr)

    fields = []
    seen   = set()

    for m in FIELD_REF_PATTERN.finditer(clean):
        full_token = m.group(0)  # may include alias prefix
        field_name = m.group(1).upper()

        # Skip SQL keywords, numbers, single chars used as operators
        if field_name in SQL_KEYWORDS:
            continue
        if re.fullmatch(r'\d+', field_name):
            continue
        if len(field_name) == 1 and not re.match(r'[A-Z]', field_name):
            continue

        if field_name not in seen:
            seen.add(field_name)
            fields.append(field_name)

    return fields


def extract_case_values(expr: str) -> str:
    """Extract THEN / ELSE result values from a CASE expression."""
    values = []
    seen   = set()

    for m in THEN_PATTERN.finditer(expr):
        val = m.group(1).strip().strip("'")
        if val.upper() not in SQL_KEYWORDS and val not in seen:
            seen.add(val)
            values.append(val)

    for m in ELSE_PATTERN.finditer(expr):
        val = m.group(1).strip().strip("'")
        if val.upper() not in SQL_KEYWORDS and val not in seen:
            seen.add(val)
            values.append(val)

    return '; '.join(values)


def resolve_source_tables(fields: list, tables: dict) -> str:
    """
    Given a list of field references from the expression and the alias map,
    return a deduplicated list of source table full names.
    """
    # Re-scan the raw expression for alias.field patterns to get table aliases
    return ''   # filled in by process_statement after we have the alias map


def resolve_tables_from_expr(raw_expr: str, alias_map: dict) -> str:
    """
    Look for alias.field patterns in the expression, resolve aliases to
    full table names, and return a deduplicated comma-separated string.
    """
    clean   = strip_literals(strip_comments(raw_expr))
    pattern = re.compile(r'\b([a-zA-Z_]\w*)\.([a-zA-Z_]\w*)\b')

    found = []
    seen  = set()

    for m in pattern.finditer(clean):
        alias = m.group(1).upper()
        # Skip things like SCHEMA.TABLE references that aren't aliases
        full = alias_map.get(alias, '')
        if full and full not in seen:
            seen.add(full)
            found.append(full)

    # If no alias.field found but there's only one table, assign it
    if not found and len(alias_map) == 1:
        return next(iter(alias_map.values()))

    return ', '.join(found)


# ---------------------------------------------------------------------------
# Core column parser
# ---------------------------------------------------------------------------

def parse_column(col_expr: str, alias_map: dict) -> dict:
    """
    Parse a single column expression and return a data dictionary row dict.
    """
    # Normalize whitespace, strip inline comments
    col_expr = strip_comments(col_expr)
    col_expr = normalize(col_expr)

    if not col_expr:
        return None

    # Handle SELECT *
    if col_expr.strip() == '*' or re.match(r'^[a-zA-Z_]\w*\.\*$', col_expr.strip()):
        return None

    # Extract alias and raw expression
    output_col, raw_expr = extract_alias(col_expr)

    # Determine if derived
    derived = '*' if detect_derived(raw_expr) else ''

    # Extract base fields
    base_fields = extract_base_fields(raw_expr)

    # Resolve source tables
    source_tables = resolve_tables_from_expr(raw_expr, alias_map)

    # Extract key/possible values from CASE
    key_values = ''
    if CASE_PATTERN.search(raw_expr):
        key_values = extract_case_values(raw_expr)

    # Logic is the raw expression if derived
    logic = normalize(raw_expr) if derived else ''

    return {
        "Output Column Name":        output_col,
        "Original Request Field Name": normalize(raw_expr),
        "Short Description":         '',
        "Long Description":          '',
        "Key / Possible Values":     key_values,
        "Derived":                   derived,
        "Base Field(s)":             ', '.join(base_fields),
        "Source Table(s)":           source_tables,
        "Logic":                     logic,
    }


# ---------------------------------------------------------------------------
# Statement processor
# ---------------------------------------------------------------------------

def process_statement(stmt_text: str) -> list:
    """
    Parse one SELECT statement (which may contain UNION ALL) and return
    a list of data dictionary row dicts.
    """
    parts     = split_on_union(stmt_text)
    first_sql = parts[0]

    # Collect table references from ALL union parts
    all_tables_sql = ' '.join(parts)
    alias_map      = extract_table_references(all_tables_sql)

    # Get the SELECT column list from the FIRST part only
    select_clause = get_select_clause(first_sql)
    if not select_clause:
        return []

    columns = split_column_list(select_clause)
    rows    = []

    for col_expr in columns:
        row = parse_column(col_expr, alias_map)
        if row:
            rows.append(row)

    return rows


# ---------------------------------------------------------------------------
# File I/O
# ---------------------------------------------------------------------------

def read_sql_file(path: str) -> str:
    with open(path, 'r', encoding='utf-8-sig') as f:
        return f.read()


def write_csv(rows: list, output_path: str):
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) < 2:
        print("Usage: python sql_to_data_dict.py input.sql [output.csv]")
        sys.exit(1)

    input_path = sys.argv[1]

    if not os.path.exists(input_path):
        print(f"Error: File not found: {input_path}")
        sys.exit(1)

    if len(sys.argv) >= 3:
        output_path = sys.argv[2]
    else:
        base        = os.path.splitext(os.path.basename(input_path))[0]
        output_path = os.path.join(
            os.path.dirname(os.path.abspath(input_path)),
            f"{base}_data_dictionary.csv"
        )

    sql_text   = read_sql_file(input_path)
    statements = sqlparse.split(sql_text)

    all_rows       = []
    select_count   = 0
    skipped_count  = 0

    for raw_stmt in statements:
        raw_stmt = raw_stmt.strip()
        if not raw_stmt:
            continue

        if is_session_init(raw_stmt) or not contains_select(raw_stmt):
            skipped_count += 1
            continue

        rows = process_statement(raw_stmt)
        if rows:
            select_count += 1
            all_rows.extend(rows)

    write_csv(all_rows, output_path)

    print(f"Statements skipped (session init): {skipped_count}")
    print(f"SELECT statements processed:       {select_count}")
    print(f"Columns extracted:                 {len(all_rows)}")
    print(f"Output saved: {output_path}")


if __name__ == '__main__':
    main()
