"""
SQL Data Dictionary Extractor  v3  (Snowflake-compatible)

Handles:
  - Snowflake SQL: /* */ and -- comments, :: casting, :field semi-structured,
    double-quoted identifiers, $N positional references, QUALIFY clause
  - Nested SELECT / subqueries  (recursive table collection)
  - CTEs  (WITH ... AS (...))
  - UNION / UNION ALL
  - Analytics / window functions: RANK, DENSE_RANK, ROW_NUMBER, NTILE,
    LEAD, LAG, FIRST_VALUE, LAST_VALUE, SUM/COUNT/AVG OVER (...),
    LISTAGG ... WITHIN GROUP (ORDER BY ...) etc.
    PARTITION BY, ORDER BY, and frame clauses are captured in the Logic field.
  - Session-initialization statements are skipped automatically

Usage:
    python sql_to_data_dict.py  input.sql  [output.csv]
"""

import sys
import csv
import re
import os
import argparse

try:
    import sqlparse
except ImportError:
    print("Error: sqlparse is required.  Run:  pip install sqlparse")
    sys.exit(1)


# ── CSV schema ────────────────────────────────────────────────────────────────

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


# ── Keyword sets ──────────────────────────────────────────────────────────────

# First keyword of statements that should be skipped
SESSION_KEYWORDS = frozenset({
    "SET", "USE", "DECLARE", "ALTER", "GO", "EXEC", "EXECUTE",
    "DROP", "TRUNCATE", "GRANT", "REVOKE", "BEGIN", "COMMIT",
    "ROLLBACK", "PRINT", "RAISERROR", "THROW", "IF", "WHILE",
    "RETURN", "INSERT", "UPDATE", "DELETE",
})

# Words that must not be treated as field or table names
SQL_KEYWORDS = frozenset({
    # DML / clause keywords
    "CASE", "WHEN", "THEN", "ELSE", "END", "AND", "OR", "NOT", "NULL",
    "AS", "IN", "IS", "BETWEEN", "LIKE", "ILIKE", "RLIKE", "TRUE", "FALSE",
    "EXISTS", "ANY", "ALL", "SOME",
    "SELECT", "FROM", "WHERE", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER",
    "FULL", "CROSS", "LATERAL", "ON", "USING", "GROUP", "BY", "ORDER",
    "HAVING", "QUALIFY", "OVER", "PARTITION", "ROWS", "RANGE", "DISTINCT",
    "TOP", "LIMIT", "OFFSET", "FETCH", "NEXT", "ONLY",
    "UNION", "EXCEPT", "INTERSECT", "WITH", "RECURSIVE",
    "PIVOT", "UNPIVOT", "SAMPLE", "TABLESAMPLE", "FLATTEN", "TABLE",
    "ASC", "DESC", "NULLS", "FIRST", "LAST",
    "PRECEDING", "FOLLOWING", "UNBOUNDED", "CURRENT", "ROW",
    "WITHIN", "GROUP",          # LISTAGG ... WITHIN GROUP (ORDER BY ...)
    # Snowflake / ANSI functions (commonly appear as bare words)
    "CAST", "TRY_CAST", "CONVERT", "COALESCE", "NULLIF", "IFF", "IIF",
    "DECODE", "NVL", "NVL2", "ZEROIFNULL", "IFNULL",
    "COUNT", "SUM", "AVG", "MIN", "MAX", "MEDIAN", "STDDEV", "VARIANCE",
    "ROW_NUMBER", "RANK", "DENSE_RANK", "LEAD", "LAG", "NTILE",
    "FIRST_VALUE", "LAST_VALUE", "NTH_VALUE", "CUME_DIST", "PERCENT_RANK",
    "LISTAGG", "ARRAY_AGG", "OBJECT_AGG", "BOOLOR_AGG", "BOOLAND_AGG",
    "GETDATE", "NOW", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
    "SYSDATE", "TODAY",
    "DATEADD", "DATEDIFF", "DATEPART", "DATE_PART", "DATE_TRUNC", "TRUNC",
    "YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND",
    "QUARTER", "WEEK", "DAYOFWEEK", "DAYOFYEAR",
    "TO_DATE", "TO_TIMESTAMP", "TO_TIME", "TO_CHAR", "TO_NUMBER",
    "TRY_TO_DATE", "TRY_TO_TIMESTAMP", "TRY_TO_NUMBER",
    "SUBSTRING", "SUBSTR", "LEFT", "RIGHT", "LEN", "LENGTH",
    "TRIM", "LTRIM", "RTRIM", "UPPER", "LOWER", "INITCAP",
    "REPLACE", "REGEXP_REPLACE", "CONCAT", "CONCAT_WS", "SPLIT_PART",
    "CHARINDEX", "POSITION", "CONTAINS", "STARTSWITH", "ENDSWITH",
    "ROUND", "FLOOR", "CEILING", "CEIL", "ABS", "SIGN",
    "POWER", "SQRT", "MOD", "RANDOM", "UNIFORM", "GREATEST", "LEAST",
    "PARSE_JSON", "GET", "GET_PATH", "OBJECT_CONSTRUCT",
    "ARRAY_CONSTRUCT", "ARRAY_SIZE", "ARRAY_CONTAINS",
    "TYPEOF", "CHECK_JSON", "IS_NULL_VALUE",
    # Type names (Snowflake)
    "VARCHAR", "NUMBER", "INTEGER", "INT", "BIGINT", "SMALLINT",
    "FLOAT", "DOUBLE", "BOOLEAN", "DATE", "TIME", "TIMESTAMP",
    "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ",
    "VARIANT", "ARRAY", "OBJECT", "BINARY", "TEXT", "STRING", "CHAR",
    # Common value literals that appear as tokens
    "YES", "NO", "UNKNOWN", "ACTIVE", "CLOSED",
    "HIGH", "MEDIUM", "LOW", "OTHER", "NONE",
})

# Aliases that look like table aliases but are actually clause keywords
CLAUSE_WORDS = frozenset({
    "WHERE", "ON", "AND", "OR", "SET", "WHEN", "THEN", "ELSE", "END",
    "BY", "HAVING", "QUALIFY", "UNION", "ALL", "SELECT", "WITH",
    "LEFT", "RIGHT", "INNER", "OUTER", "CROSS", "FULL", "JOIN", "AS",
    "IN", "NOT", "IS", "NULL", "BETWEEN", "LIKE", "EXISTS", "LATERAL",
    "OVER", "PARTITION", "ROWS", "RANGE", "LIMIT", "OFFSET",
    "PIVOT", "UNPIVOT", "SAMPLE", "TABLESAMPLE",
    "GROUP", "ORDER", "FETCH", "NEXT", "ONLY", "FIRST", "LAST",
})


# ── Compiled patterns ─────────────────────────────────────────────────────────

INLINE_COMMENT = re.compile(r'--[^\n]*')
BLOCK_COMMENT  = re.compile(r'/\*.*?\*/', re.DOTALL)
STRING_LITERAL = re.compile(r"'(?:[^'\\]|\\.)*'")   # single-quoted strings

# FROM/JOIN <table> [AS] [alias]
# Excludes subqueries with (?!\()
# Handles: db.schema.table, "Quoted"."Name", $staging_table
TABLE_REF_PATTERN = re.compile(
    r'(?:FROM|JOIN)\s+'
    r'(?!\()(?!\bLATERAL\b)'          # not a subquery, not LATERAL alone
    r'('
        r'(?:"[^"]+"|[a-zA-Z_$][\w$]*)'
        r'(?:\.(?:"[^"]+"|[a-zA-Z_$][\w$]*))*'
    r')'
    r'(?:\s+(?:AS\s+)?'
        r'(?!(?:WHERE|ON|SET|AND|OR|INNER|LEFT|RIGHT|FULL|CROSS|OUTER'
             r'|JOIN|GROUP|ORDER|HAVING|QUALIFY|UNION|EXCEPT|INTERSECT'
             r'|LIMIT|OFFSET|PIVOT|UNPIVOT|SAMPLE)\b)'
        r'([a-zA-Z_$][\w$]*))?',
    re.IGNORECASE,
)

# Comma-separated table in a FROM list: , table2 [AS] [alias]
# Applied only within extracted FROM-clause segments to avoid SELECT-list false positives
COMMA_TABLE_RE = re.compile(
    r',\s+'
    r'(?!\()(?!\bLATERAL\b)'
    r'('
        r'(?:"[^"]+"|[a-zA-Z_$][\w$]*)'
        r'(?:\.(?:"[^"]+"|[a-zA-Z_$][\w$]*))*'
    r')'
    r'(?:\s+(?:AS\s+)?'
        r'(?!(?:WHERE|ON|SET|AND|OR|INNER|LEFT|RIGHT|FULL|CROSS|OUTER'
             r'|JOIN|GROUP|ORDER|HAVING|QUALIFY|UNION|EXCEPT|INTERSECT'
             r'|LIMIT|OFFSET|PIVOT|UNPIVOT|SAMPLE|SELECT)\b)'
        r'([a-zA-Z_$][\w$]*))?',
    re.IGNORECASE,
)

# Keywords that end a FROM clause at depth 0
_FROM_ENDERS = re.compile(
    r'\b(?:WHERE|GROUP|HAVING|ORDER|QUALIFY|UNION|EXCEPT|INTERSECT'
    r'|LIMIT|OFFSET|FETCH)\b',
    re.IGNORECASE,
)

# CTE definitions: WITH name AS (  or  , name AS (
CTE_DEF_PATTERN = re.compile(
    r'(?:WITH|,)\s*(?:"([^"]+)"|([a-zA-Z_$][\w$]*))\s+AS\s*\(',
    re.IGNORECASE,
)

# AS alias at the END of a column expression (outside parens)
ALIAS_PATTERN = re.compile(
    r'\bAS\s+("(?:[^"]+)"|[a-zA-Z_$][\w$]*)\s*$',
    re.IGNORECASE,
)

# alias.field references inside expressions
ALIASED_FIELD = re.compile(r'\b([a-zA-Z_$][\w$]*)\.([a-zA-Z_$][\w$]*)\b')

# Any word token (for base field extraction)
WORD_TOKEN = re.compile(r'\b([a-zA-Z_$][\w$]*)\b|\$\d+')

# Derived-expression detectors
CASE_RE    = re.compile(r'\bCASE\b',        re.IGNORECASE)
FUNC_RE    = re.compile(r'\b[\w$]+\s*\(',   re.IGNORECASE)
ARITH_RE   = re.compile(r'(?<![<>!=\-])[+\-*/](?![>=])')
CONCAT_RE  = re.compile(r'\|\|')
SF_CAST_RE = re.compile(r'::[a-zA-Z_][\w]*')   # Snowflake :: cast
SF_SEMI_RE = re.compile(r':[a-zA-Z_$][\w$]*')  # :field  semi-structured

# Window / analytics function detectors
OVER_RE         = re.compile(r'\bOVER\s*\(',                       re.IGNORECASE)
WITHIN_GROUP_RE = re.compile(r'\bWITHIN\s+GROUP\s*\(',            re.IGNORECASE)
PARTITION_BY_RE = re.compile(r'\bPARTITION\s+BY\s+(.+?)(?=\bORDER\b|\bROWS\b|\bRANGE\b|$)', re.IGNORECASE | re.DOTALL)
ORDER_BY_RE     = re.compile(r'\bORDER\s+BY\s+(.+?)(?=\bROWS\b|\bRANGE\b|$)',                re.IGNORECASE | re.DOTALL)
FRAME_RE        = re.compile(r'\b(?:ROWS|RANGE)\s+BETWEEN\s+.+',  re.IGNORECASE)

# Window function names (used to identify and label analytic columns)
WINDOW_FUNC_NAMES = frozenset({
    "RANK", "DENSE_RANK", "ROW_NUMBER", "NTILE",
    "PERCENT_RANK", "CUME_DIST",
    "LEAD", "LAG", "FIRST_VALUE", "LAST_VALUE", "NTH_VALUE",
    "SUM", "COUNT", "AVG", "MIN", "MAX", "MEDIAN", "STDDEV", "VARIANCE",
    "LISTAGG", "ARRAY_AGG", "OBJECT_AGG",
    "RATIO_TO_REPORT",
})

# THEN / ELSE values inside CASE
THEN_RE = re.compile(r"\bTHEN\s+('(?:[^'\\]|\\.)*'|\d+(?:\.\d+)?|[A-Za-z_]\w*)", re.IGNORECASE)
ELSE_RE = re.compile(r"\bELSE\s+('(?:[^'\\]|\\.)*'|\d+(?:\.\d+)?|[A-Za-z_]\w*)", re.IGNORECASE)


# ── Text utilities ────────────────────────────────────────────────────────────

def strip_comments(sql: str) -> str:
    """
    Remove -- line comments and /* */ block comments while preserving
    the content of single-quoted and double-quoted string literals.
    Blindly applying a regex would corrupt expressions like
    REPLACE(col, '--', '') or REPLACE(col, '/*', '').
    """
    result = []
    i = 0
    n = len(sql)
    in_str  = False
    str_ch  = None

    while i < n:
        ch  = sql[i]
        two = sql[i:i + 2]

        if in_str:
            result.append(ch)
            # SQL standard doubling escape: '' or ""
            if ch == str_ch:
                if i + 1 < n and sql[i + 1] == str_ch:
                    result.append(sql[i + 1])
                    i += 2
                    continue
                in_str = False
        elif two == '/*':
            end = sql.find('*/', i + 2)
            result.append(' ')
            i = end + 2 if end != -1 else n
            continue
        elif two == '--':
            end = sql.find('\n', i + 2)
            result.append(' ')
            i = end if end != -1 else n
            continue
        elif ch in ("'", '"'):
            in_str, str_ch = True, ch
            result.append(ch)
        else:
            result.append(ch)

        i += 1

    return ''.join(result)

def strip_literals(sql: str) -> str:
    return STRING_LITERAL.sub("''", sql)

def norm(sql: str) -> str:
    return re.sub(r'\s+', ' ', sql).strip()


# ── Statement helpers ─────────────────────────────────────────────────────────

def first_keyword(text: str) -> str:
    clean = strip_comments(text).strip()
    m = re.match(r'([a-zA-Z_$][\w$]*)', clean)
    return m.group(1).upper() if m else ''

def is_session_init(stmt: str) -> bool:
    clean = strip_comments(stmt).strip()
    if not clean:
        return True
    kw = first_keyword(clean)
    if kw in SESSION_KEYWORDS:
        return True
    # CREATE without a SELECT inside is DDL, not a data query
    if kw == 'CREATE' and not re.search(r'\bSELECT\b', clean, re.IGNORECASE):
        return True
    return False

def contains_select(stmt: str) -> bool:
    return bool(re.search(r'\bSELECT\b', strip_comments(stmt), re.IGNORECASE))


# ── Parenthesis / depth utilities ─────────────────────────────────────────────

def paren_contents(sql: str) -> list:
    """
    Return the inner text of every top-level parenthesised block.
    Inner nesting is preserved inside each returned string.
    """
    results, depth, start = [], 0, -1
    in_str, str_ch = False, None
    i = 0
    while i < len(sql):
        ch = sql[i]
        if not in_str and ch in ("'", '"'):
            in_str, str_ch = True, ch
        elif in_str:
            if ch == str_ch and sql[i - 1:i] != '\\':
                in_str = False
        elif ch == '(':
            if depth == 0:
                start = i + 1
            depth += 1
        elif ch == ')':
            depth -= 1
            if depth == 0 and start >= 0:
                results.append(sql[start:i])
                start = -1
        i += 1
    return results


def split_depth0(sql: str, keyword: str) -> list:
    """Split sql on keyword only at parenthesis depth 0."""
    parts, start = [], 0
    depth, in_str, str_ch = 0, False, None
    klen  = len(keyword)
    upper = sql.upper()
    i     = 0
    while i < len(sql):
        ch = sql[i]
        if not in_str and ch in ("'", '"'):
            in_str, str_ch = True, ch
        elif in_str:
            if ch == str_ch and sql[i - 1:i] != '\\':
                in_str = False
        elif ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
        elif depth == 0 and not in_str:
            seg   = upper[i:i + klen]
            after = upper[i + klen: i + klen + 1]
            if seg == keyword and (not after or not (after.isalnum() or after == '_')):
                parts.append(sql[start:i].strip())
                i += klen
                start = i
                continue
        i += 1
    parts.append(sql[start:].strip())
    return [p for p in parts if p]


def split_union(sql: str) -> list:
    """Split on UNION ALL / UNION at depth 0, ignoring occurrences in comments."""
    clean = strip_comments(sql)   # remove -- and /* */ before splitting
    parts = split_depth0(clean, 'UNION ALL')
    return parts if len(parts) > 1 else split_depth0(clean, 'UNION')


# ── CTE extraction ────────────────────────────────────────────────────────────

def cte_names(sql: str) -> set:
    """Return upper-case names of all CTEs defined in a WITH clause."""
    names = set()
    for m in CTE_DEF_PATTERN.finditer(strip_comments(sql)):
        name = (m.group(1) or m.group(2)).upper().strip('"')
        names.add(name)
    return names


# ── Recursive table collection ────────────────────────────────────────────────

def _flatten_parens(text: str) -> str:
    """Replace content inside every parenthesised block with spaces.
    Used so that comma-table scanning never looks inside subqueries."""
    result = []
    depth = 0
    for ch in text:
        if ch == '(':
            depth += 1
            result.append('(')
        elif ch == ')':
            depth -= 1
            result.append(')')
        elif depth > 0:
            result.append(' ')
        else:
            result.append(ch)
    return ''.join(result)


def _extract_from_clauses(clean_sql: str) -> list:
    """
    Return a list of FROM-clause text segments with paren content blanked out,
    so COMMA_TABLE_RE can safely find comma-separated table lists without
    accidentally matching column lists inside inline subqueries.
    """
    flat = _flatten_parens(clean_sql)   # blank out all paren content first
    segments = []
    upper = flat.upper()
    i = 0
    n = len(flat)
    while i < n:
        if upper[i:i + 4] == 'FROM':
            b = flat[i - 1] if i > 0 else ' '
            a = flat[i + 4] if i + 4 < n else ' '
            if not (b.isalnum() or b in '_$') and not (a.isalnum() or a in '_$'):
                start = i + 4
                j = start
                end = n
                while j < n:
                    c = flat[j]
                    if c == ')':          # hit a closing paren at depth 0 → end of FROM
                        end = j
                        break
                    elif _FROM_ENDERS.match(flat, j):
                        end = j
                        break
                    j += 1
                segments.append(flat[start:end])
                i = end
                continue
        i += 1
    return segments


def _subquery_aliases(clean_sql: str) -> list:
    """
    Find (subquery) [AS] alias patterns at depth 0.
    Returns list of (paren_content, alias_upper) for each inline view.
    """
    results = []
    upper  = clean_sql.upper()
    depth  = 0
    paren_start = -1
    i = 0
    n = len(clean_sql)

    while i < n:
        ch = clean_sql[i]
        if ch == '(':
            if depth == 0:
                paren_start = i + 1
            depth += 1
        elif ch == ')':
            depth -= 1
            if depth == 0 and paren_start >= 0:
                content = clean_sql[paren_start:i]
                if re.search(r'\bSELECT\b', content, re.IGNORECASE):
                    # Look for optional AS + alias after the closing paren
                    j = i + 1
                    while j < n and clean_sql[j] in ' \t\n\r':
                        j += 1
                    if upper[j:j + 2] == 'AS' and not (clean_sql[j + 2:j + 3].isalnum()
                                                         or clean_sql[j + 2:j + 3] == '_'):
                        j += 2
                        while j < n and clean_sql[j] in ' \t\n\r':
                            j += 1
                    alias_m = re.match(r'[a-zA-Z_$][\w$]*', clean_sql[j:])
                    if alias_m:
                        alias = alias_m.group(0).upper()
                        if alias not in CLAUSE_WORDS:
                            results.append((content, alias))
                paren_start = -1
        i += 1
    return results


def _register_table(full: str, alias, skip_names: set, result: dict):
    """Add a single table reference (full name + optional alias) to result dict."""
    full = full.strip('"')
    if full.upper().strip('"') in skip_names:
        return
    if full.split('.')[-1].upper().strip('"') in skip_names:
        return
    if alias and (alias.upper() in CLAUSE_WORDS or alias.upper() in skip_names):
        alias = None

    key = (alias or full.split('.')[-1]).upper().strip('"')
    if key not in CLAUSE_WORDS and key not in skip_names:
        result[key] = full

    short = full.split('.')[-1].upper().strip('"')
    if short not in CLAUSE_WORDS and short not in skip_names:
        result.setdefault(short, full)

    result[full.upper().strip('"')] = full


def _tables_at_level(sql: str, skip_names: set) -> dict:
    """
    Collect physical table references at THIS level only.
    skip_names = CTE names + clause keywords (should not be treated as tables).
    Returns: {alias_upper: full_name}
    """
    clean  = strip_literals(strip_comments(sql))
    result = {}

    # Primary scan: FROM / JOIN table references
    for m in TABLE_REF_PATTERN.finditer(clean):
        _register_table(m.group(1), m.group(2), skip_names, result)

    # Secondary scan: comma-separated tables within each FROM clause
    # e.g.  FROM t1 a, t2 b  (implicit cross-join syntax)
    for segment in _extract_from_clauses(clean):
        for m in COMMA_TABLE_RE.finditer(segment):
            _register_table(m.group(1), m.group(2), skip_names, result)

    return result


def all_tables(sql: str, parent_ctes: set = None, _depth: int = 0) -> dict:
    """
    Recursively collect every physical table touched by sql, including
    tables inside subqueries and CTE bodies.
    Also registers inline-view aliases so that outer-query references like
    sub1.COLUMN correctly resolve their source table.
    Returns {alias_upper: full_name}
    """
    if _depth > 20:
        return {}
    if parent_ctes is None:
        parent_ctes = set()

    clean_sql  = strip_literals(strip_comments(sql))
    this_ctes  = cte_names(sql)
    all_ctes   = parent_ctes | this_ctes
    skip       = all_ctes | CLAUSE_WORDS

    tables = _tables_at_level(sql, skip)

    # Recurse into every parenthesised block that contains SELECT,
    # and register derived-table aliases so the outer query can resolve them.
    for content, alias in _subquery_aliases(clean_sql):
        inner = all_tables(content, all_ctes, _depth + 1)
        # Merge inner tables into the map
        for k, v in inner.items():
            tables.setdefault(k, v)
        # Register the inline-view alias → comma-separated physical tables
        if alias and alias not in skip:
            physical = list(dict.fromkeys(
                v for k, v in inner.items()
                if '.' in v or v.upper() == k
            ))
            if physical:
                tables[alias] = ', '.join(physical)

    # Also recurse into any paren blocks not already covered above
    # (e.g. scalar subqueries, CTE bodies without an alias)
    for content in paren_contents(sql):
        if re.search(r'\bSELECT\b', content, re.IGNORECASE):
            for k, v in all_tables(content, all_ctes, _depth + 1).items():
                tables.setdefault(k, v)

    return tables


# ── SELECT-clause extraction ──────────────────────────────────────────────────

def get_raw_select_clause(sql: str) -> str:
    """
    Return the raw text (with -- and /* */ comments preserved) between the
    first top-level SELECT and the first top-level FROM.
    Handles inline comments during position tracking so they never interfere.
    """
    in_str, str_ch = False, None
    in_line_cmt, in_block_cmt = False, False
    depth, sel_end = 0, -1
    i = 0
    upper = sql.upper()

    # Phase 1: find SELECT at depth 0, ignoring comments
    while i < len(sql):
        ch = sql[i]
        two = sql[i:i + 2]
        if in_block_cmt:
            if two == '*/':
                in_block_cmt = False; i += 2; continue
        elif in_line_cmt:
            if ch == '\n': in_line_cmt = False
        elif in_str:
            if ch == str_ch: in_str = False
        elif two == '/*': in_block_cmt = True; i += 2; continue
        elif two == '--': in_line_cmt = True
        elif ch in ("'", '"'): in_str, str_ch = True, ch
        elif ch == '(': depth += 1
        elif ch == ')': depth -= 1
        elif depth == 0 and not in_line_cmt:
            if upper[i:i + 6] == 'SELECT':
                b = sql[i - 1] if i > 0 else ' '
                a = sql[i + 6] if i + 6 < len(sql) else ' '
                if not (b.isalnum() or b in '_$') and \
                   not (a.isalnum() or a in '_$'):
                    sel_end = i + 6; break
        i += 1

    if sel_end < 0:
        return ''

    rest = sql[sel_end:]
    rest = re.sub(r'^\s*(DISTINCT|ALL)\b', '', rest, flags=re.IGNORECASE)
    rest = re.sub(r'^\s*TOP\s+\d+\b',      '', rest, flags=re.IGNORECASE)

    # Phase 2: find FROM at depth 0
    in_str, str_ch = False, None
    in_line_cmt, in_block_cmt = False, False
    depth = 0
    upper = rest.upper()
    i = 0

    while i < len(rest):
        ch = rest[i]
        two = rest[i:i + 2]
        if in_block_cmt:
            if two == '*/':
                in_block_cmt = False; i += 2; continue
        elif in_line_cmt:
            if ch == '\n': in_line_cmt = False
        elif in_str:
            if ch == str_ch: in_str = False
        elif two == '/*': in_block_cmt = True; i += 2; continue
        elif two == '--': in_line_cmt = True
        elif ch in ("'", '"'): in_str, str_ch = True, ch
        elif ch == '(': depth += 1
        elif ch == ')': depth -= 1
        elif depth == 0 and not in_line_cmt:
            if upper[i:i + 4] == 'FROM':
                b = rest[i - 1] if i > 0 else ' '
                a = rest[i + 4] if i + 4 < len(rest) else ' '
                if not (b.isalnum() or b in '_$') and \
                   not (a.isalnum() or a in '_$'):
                    return rest[:i]
        i += 1

    return rest


def split_columns_raw(raw_clause: str) -> list:
    """
    Split a SELECT column list by depth-0 commas, preserving inline comments.

    Key behaviour: when a comma is followed by a '-- comment' on the SAME LINE,
    that comment is included in the CURRENT column (not the next), because it
    describes the column that just ended:

        m.MEMBER_ID,   -- Unique member identifier   ← attached to MEMBER_ID
        m.FIRST_NM,    -- First name

    -- ... newline  and  /* ... */  are treated as non-splitting.
    Returns list of raw column strings (comments intact).
    """
    cols, cur = [], []
    depth = 0
    in_str, str_ch = False, None
    in_line_cmt, in_block_cmt = False, False
    i = 0
    n = len(raw_clause)

    while i < n:
        ch = raw_clause[i]
        two = raw_clause[i:i + 2]

        if in_block_cmt:
            cur.append(ch)
            if two == '*/':
                cur.append(raw_clause[i + 1])
                in_block_cmt = False; i += 2; continue
        elif in_line_cmt:
            cur.append(ch)
            if ch == '\n': in_line_cmt = False
        elif in_str:
            cur.append(ch)
            if ch == str_ch: in_str = False
        elif two == '/*':
            in_block_cmt = True; cur.append(ch)
        elif two == '--':
            in_line_cmt = True; cur.append(ch)
        elif ch in ("'", '"'):
            in_str, str_ch = True, ch; cur.append(ch)
        elif ch == '(':
            depth += 1; cur.append(ch)
        elif ch == ')':
            depth -= 1; cur.append(ch)
        elif ch == ',' and depth == 0:
            # Peek ahead on the same line for a trailing -- comment.
            # If found, absorb it into the current column before splitting.
            j = i + 1
            while j < n and raw_clause[j] in (' ', '\t'):
                j += 1
            if raw_clause[j:j + 2] == '--':
                # Read to end of line and add to current column
                eol = raw_clause.find('\n', j)
                eol = eol if eol != -1 else n
                cur.append(' ')
                cur.extend(raw_clause[j:eol])
                i = eol          # resume after the absorbed comment
            col = ''.join(cur).strip()
            if col: cols.append(col)
            cur = []
        else:
            cur.append(ch)
        i += 1

    col = ''.join(cur).strip()
    if col: cols.append(col)
    return cols


def _looks_like_sql(text: str) -> bool:
    """
    Return True if a comment string looks like commented-out SQL code
    rather than a human-readable description.
    Heuristics:
      - starts with a SQL keyword
      - contains alias.field dot notation
      - contains SQL operators, function calls, or quoted literals
    """
    t = text.strip()
    if not t:
        return False
    first = re.match(r'^(\w+)', t)
    if first and first.group(1).upper() in (SQL_KEYWORDS | SESSION_KEYWORDS):
        return True
    # alias.field: require both sides to be multi-char (avoids "U.S." false positive)
    if re.search(r'\b\w{2,}\.\w{2,}', t):     return True   # alias.field
    # function call: word immediately followed by ( with no space between
    if re.search(r'\b\w+\(', t):              return True   # func(
    # SQL assignment: = 'value'  (only inside SQL, not plain English "equals 'term'")
    if re.search(r'(?<!\s)=\s*\'', t):        return True   # ='value' (no space before =)
    # AND/OR/NOT only flagged if they are surrounded by identifiers (SQL context)
    # Plain English "and", "or" should not be flagged — require uppercase to indicate SQL
    if re.search(r'\b(AND|OR)\b', t):         return True   # uppercase AND/OR = SQL
    return False


def extract_inline_comment(raw_col: str) -> str:
    """
    Extract the most relevant -- comment from a raw column expression
    and return it as the Short Description.

    Priority:
    1. Trailing inline comment on the last code line:
           m.MEMBER_ID,  -- Unique member identifier
    2. Leading pure-comment line that is plain English (section header):
           -- Delinquency flag
           a.CAD_IND,

    Commented-out SQL code is detected and ignored:
           -- CASE WHEN a.X = 1 THEN 'Y' END,   ← skipped
           -- m.OLD_FIELD,                         ← skipped
    """
    lines = raw_col.split('\n')
    section_comment = ''

    # Collect first pure-comment line that is NOT commented-out SQL
    for line in lines:
        stripped = line.strip()
        if stripped.startswith('--'):
            text = stripped[2:].strip()
            if text and not _looks_like_sql(text):
                section_comment = text
                break

    # Check the last non-empty, non-comment line for a trailing --
    for line in reversed(lines):
        stripped = line.strip()
        if not stripped or stripped.startswith('--'):
            continue
        in_str, str_ch = False, None
        for j, c in enumerate(line):
            if not in_str and c in ("'", '"'):
                in_str, str_ch = True, c
            elif in_str and c == str_ch:
                in_str = False
            elif not in_str and line[j:j + 2] == '--':
                comment = line[j + 2:].strip()
                if comment and not _looks_like_sql(comment):
                    return comment        # inline description takes priority
        break

    return section_comment


def get_select_clause(sql: str) -> str:
    """
    Return the text between the first TOP-LEVEL SELECT keyword and the
    first top-level FROM keyword.

    'Top-level' means at parenthesis depth 0, so SELECT keywords inside
    CTE bodies or subqueries (which are wrapped in parentheses) are skipped.
    This correctly handles WITH ... AS (...) CTE patterns.
    """
    clean = strip_comments(sql)
    upper = clean.upper()
    depth, in_str, str_ch = 0, False, None
    sel_end = -1
    i = 0

    # Phase 1: find the first SELECT at depth 0
    while i < len(clean):
        ch = clean[i]
        if not in_str and ch in ("'", '"'):
            in_str, str_ch = True, ch
        elif in_str:
            if ch == str_ch and clean[i - 1:i] != '\\':
                in_str = False
        elif ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
        elif depth == 0 and not in_str:
            if upper[i:i + 6] == 'SELECT':
                b = clean[i - 1] if i > 0 else ' '
                a = clean[i + 6] if i + 6 < len(clean) else ' '
                if not (b.isalnum() or b in '_$') and \
                   not (a.isalnum() or a in '_$'):
                    sel_end = i + 6
                    break
        i += 1

    if sel_end < 0:
        return ''

    rest = clean[sel_end:]
    rest = re.sub(r'^\s*(DISTINCT|ALL)\b', '', rest, flags=re.IGNORECASE)
    rest = re.sub(r'^\s*TOP\s+\d+\b',      '', rest, flags=re.IGNORECASE)

    # Phase 2: find FROM at depth 0 within the remainder
    depth, in_str, str_ch = 0, False, None
    upper = rest.upper()
    i     = 0

    while i < len(rest):
        ch = rest[i]
        if not in_str and ch in ("'", '"'):
            in_str, str_ch = True, ch
        elif in_str:
            if ch == str_ch and rest[i - 1:i] != '\\':
                in_str = False
        elif ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
        elif depth == 0 and not in_str:
            if upper[i:i + 4] == 'FROM':
                b = rest[i - 1] if i > 0         else ' '
                a = rest[i + 4] if i + 4 < len(rest) else ' '
                if not (b.isalnum() or b in '_$') and \
                   not (a.isalnum() or a in '_$'):
                    return rest[:i].strip()
        i += 1

    return rest.strip()


def split_columns(select_clause: str) -> list:
    """Split SELECT column list by commas at depth 0."""
    cols, cur = [], []
    depth, in_str, str_ch = 0, False, None

    for ch in select_clause:
        if not in_str and ch in ("'", '"'):
            in_str, str_ch = True, ch
            cur.append(ch)
        elif in_str:
            cur.append(ch)
            if ch == str_ch:
                in_str = False
        elif ch == '(':
            depth += 1;  cur.append(ch)
        elif ch == ')':
            depth -= 1;  cur.append(ch)
        elif ch == ',' and depth == 0:
            col = ''.join(cur).strip()
            if col:
                cols.append(col)
            cur = []
        else:
            cur.append(ch)

    col = ''.join(cur).strip()
    if col:
        cols.append(col)
    return cols


# ── Per-column analysis ───────────────────────────────────────────────────────

def is_derived(expr: str) -> bool:
    """True if the expression uses functions, CASE, arithmetic, casts, or window functions.
    Also true for bare string/numeric literals (hardcoded constants, not base fields)."""
    stripped = expr.strip()
    # Bare string literal: 'value'
    if STRING_LITERAL.fullmatch(stripped):
        return True
    # Bare numeric literal: 123, 45.67, -1
    if re.fullmatch(r'-?\d+(\.\d+)?', stripped):
        return True
    # Boolean / NULL literals
    if re.fullmatch(r'(?:TRUE|FALSE|NULL)', stripped, re.IGNORECASE):
        return True
    c = strip_literals(strip_comments(expr))
    return any([
        CASE_RE.search(c),
        CONCAT_RE.search(c),
        SF_CAST_RE.search(c),
        SF_SEMI_RE.search(c),
        OVER_RE.search(c),
        WITHIN_GROUP_RE.search(c),
        FUNC_RE.search(c),
        ARITH_RE.search(c),
    ])


def _flat_alias(expr: str) -> str:
    """
    Return a version of expr where content inside parentheses is replaced
    with spaces, so we can safely run ALIAS_PATTERN on it.
    """
    result = []
    depth  = 0
    for ch in expr:
        if ch == '(':
            depth += 1
            result.append(' ')
        elif ch == ')':
            depth -= 1
            result.append(' ')
        elif depth > 0:
            result.append(' ')
        else:
            result.append(ch)
    return ''.join(result)


def extract_alias(expr: str) -> tuple:
    """Return (output_col_upper, raw_expr_without_alias)."""
    expr = norm(strip_comments(expr))

    flat_m = ALIAS_PATTERN.search(_flat_alias(expr))
    if flat_m:
        alias   = flat_m.group(1).strip('"').upper()
        raw_end = flat_m.start()
        raw     = expr[:raw_end].strip()
        return alias, raw

    # No AS — derive name from the expression
    plain = re.match(r'^(?:[a-zA-Z_$][\w$]*\.)?([a-zA-Z_$][\w$]*)$', expr.strip())
    if plain:
        return plain.group(1).upper(), expr

    return expr.upper(), expr


def base_fields(expr: str) -> list:
    """
    Deduplicated base field names from an expression.

    Rules:
    - For alias.field patterns, keep only the field name (drop alias prefix).
    - Remove 3+ part qualified names (db.schema.table) entirely — they are
      table refs, not column refs.
    - Skip single-character tokens (table alias letters like m, a, b).
    - Skip tokens that were seen as alias prefixes.
    """
    clean = strip_literals(strip_comments(expr))

    # Remove 3+ part table refs (db.schema.table) so their parts don't leak in
    clean = re.sub(
        r'\b(?:[a-zA-Z_$][\w$]*\.){2,}[a-zA-Z_$][\w$]*\b',
        ' ',
        clean,
    )

    result, seen, alias_prefixes = [], set(), set()

    # Pass 1 — alias.field: collect the field; record the alias as a prefix
    for m in re.finditer(r'\b([a-zA-Z_$][\w$]*)\.([a-zA-Z_$][\w$]*)\b', clean):
        alias = m.group(1).upper()
        field = m.group(2).upper()
        alias_prefixes.add(alias)
        if field in SQL_KEYWORDS:
            continue
        if field not in seen:
            seen.add(field)
            result.append(field)

    # Pass 2 — bare words not used as alias prefixes
    for m in re.finditer(r'\b([a-zA-Z_$][\w$]*)\b|\$\d+', clean):
        raw  = m.group(0)
        name = m.group(1)

        if name is None:                    # $N positional reference — skip
            continue

        upper = name.upper()
        if upper in SQL_KEYWORDS:           continue
        if upper in alias_prefixes:         continue  # was a table alias
        if upper in seen:                   continue
        if re.fullmatch(r'\d+', name):      continue
        if len(name) == 1:                  continue  # single-char alias

        seen.add(upper)
        result.append(upper)

    return result


def case_values(expr: str) -> str:
    """THEN/ELSE result values from a CASE expression."""
    vals, seen = [], set()
    for pat in (THEN_RE, ELSE_RE):
        for m in pat.finditer(expr):
            v = m.group(1).strip().strip("'")
            if v.upper() not in SQL_KEYWORDS and v not in seen:
                seen.add(v); vals.append(v)
    return '; '.join(vals)


def source_tables(raw_expr: str, alias_map: dict,
                  derived_col_map: dict = None) -> str:
    """
    Resolve source tables for a single column expression.
    When derived_col_map is supplied, a simple alias.field reference first
    checks the inner-SELECT column map to get the precise physical table
    rather than the full list stored for the derived-table alias.
    """
    clean = strip_literals(strip_comments(raw_expr))
    found, seen = [], set()

    # aliased references: alias.field
    for m in ALIASED_FIELD.finditer(clean):
        alias = m.group(1).upper()
        field = m.group(2).upper()

        # Prefer precise lookup in derived column map
        if derived_col_map:
            inner_row = derived_col_map.get(alias, {}).get(field)
            if inner_row is not None:
                t = inner_row.get('Source Table(s)', '')
                if t and t not in seen:
                    seen.add(t); found.append(t)
                continue

        full = alias_map.get(alias, '')
        if full and full not in seen:
            seen.add(full); found.append(full)

    # scalar subquery: (SELECT ... FROM ...)
    if not found and '(' in raw_expr:
        for content in paren_contents(raw_expr):
            if re.search(r'\bSELECT\b', content, re.IGNORECASE):
                for v in all_tables(content).values():
                    if v not in seen:
                        seen.add(v); found.append(v)

    # Fallback: bare column with no alias prefix.
    # Only resolve if there is exactly ONE physical table in scope.
    if not found:
        physical = [v for k, v in alias_map.items()
                    if '.' in v or v.upper() == k]
        unique = list(dict.fromkeys(physical))
        if len(unique) == 1:
            return unique[0]

    return ', '.join(dict.fromkeys(found))


# ── Window function helpers ───────────────────────────────────────────────────

def _paren_block(text: str, start: int) -> str:
    """Return content of the parenthesised block opening at text[start]."""
    depth = 0
    for i in range(start, len(text)):
        if text[i] == '(':
            depth += 1
        elif text[i] == ')':
            depth -= 1
            if depth == 0:
                return text[start + 1: i]
    return text[start + 1:]


def parse_window_details(expr: str) -> dict:
    """
    Decompose a window / analytic function expression into its parts.

    Returns a dict with:
      func_name    – e.g. RANK, SUM, LISTAGG
      func_args    – arguments inside the function call
      partition_by – PARTITION BY column list (empty string if absent)
      order_by     – ORDER BY column list (empty string if absent)
      frame        – ROWS/RANGE BETWEEN … clause (empty string if absent)
      within_group – ORDER BY from WITHIN GROUP (for LISTAGG etc.)
    """
    result = dict(func_name='', func_args='',
                  partition_by='', order_by='', frame='', within_group='')

    clean = strip_comments(expr)

    # ── Locate the boundary before OVER / WITHIN GROUP ───────────────────────
    over_m  = OVER_RE.search(clean)
    wg_m    = WITHIN_GROUP_RE.search(clean)
    boundary = min(
        (m.start() for m in (over_m, wg_m) if m),
        default=-1,
    )
    if boundary == -1:
        return result

    func_part = clean[:boundary].strip()

    # ── Extract function name and args ────────────────────────────────────────
    fm = re.match(r'(\w[\w$]*)\s*\((.*)\)\s*$', func_part, re.DOTALL)
    if fm:
        result['func_name'] = fm.group(1).upper()
        result['func_args'] = norm(fm.group(2))

    # ── Extract WITHIN GROUP (ORDER BY ...) ───────────────────────────────────
    if wg_m:
        wg_content = _paren_block(clean, wg_m.end() - 1)
        ord_m = re.search(r'\bORDER\s+BY\s+(.+)', wg_content, re.IGNORECASE | re.DOTALL)
        if ord_m:
            result['within_group'] = norm(ord_m.group(1))

    # ── Extract OVER (...) clause ─────────────────────────────────────────────
    if over_m:
        over_content = _paren_block(clean, over_m.end() - 1)

        # PARTITION BY
        pm = PARTITION_BY_RE.search(over_content)
        if pm:
            result['partition_by'] = norm(pm.group(1)).rstrip(',').strip()

        # ORDER BY  (inside OVER, not WITHIN GROUP)
        om = ORDER_BY_RE.search(over_content)
        if om:
            result['order_by'] = norm(om.group(1)).rstrip(',').strip()

        # Frame clause
        fm2 = FRAME_RE.search(over_content)
        if fm2:
            result['frame'] = norm(fm2.group(0))

    return result


def window_logic(expr: str) -> str:
    """
    Build a human-readable Logic string for a window/analytic function.
    Falls back to the raw expression if no window structure is found.
    """
    wd = parse_window_details(expr)
    if not wd['func_name']:
        return norm(expr)

    parts = [f"Function: {wd['func_name']}({wd['func_args']})"]
    if wd['within_group']:
        parts.append(f"Within Group Order By: {wd['within_group']}")
    if wd['partition_by']:
        parts.append(f"Partition By: {wd['partition_by']}")
    if wd['order_by']:
        parts.append(f"Order By: {wd['order_by']}")
    if wd['frame']:
        parts.append(f"Frame: {wd['frame']}")

    return ' | '.join(parts)


# ── Column row builder ────────────────────────────────────────────────────────

def parse_column(col_expr: str, alias_map: dict, short_desc: str = '',
                 derived_col_map: dict = None) -> dict:
    col_expr = norm(strip_comments(col_expr))
    if not col_expr:
        return None

    # SELECT *  or  t.*
    if re.match(r'^(?:[a-zA-Z_$][\w$]*\.)?\*$', col_expr.strip()):
        return None

    output_col, raw = extract_alias(col_expr)
    derived         = '*' if is_derived(raw) else ''
    fields          = base_fields(raw)
    tables          = source_tables(raw, alias_map, derived_col_map)
    kv              = case_values(raw) if CASE_RE.search(raw) else ''

    # For a simple alias.field reference that is not itself derived, check
    # whether the inner SELECT marked that column as derived and inherit its
    # logic / key-values so derivation is not lost across inline-view layers.
    if not derived and derived_col_map:
        plain = re.match(r'^([a-zA-Z_$][\w$]*)\.([a-zA-Z_$][\w$]*)$', raw.strip())
        if plain:
            outer_alias = plain.group(1).upper()
            field_name  = plain.group(2).upper()
            inner_row   = derived_col_map.get(outer_alias, {}).get(field_name)
            if inner_row and inner_row.get('Derived') == '*':
                derived = '*'
                if not kv:
                    kv = inner_row.get('Key / Possible Values', '')
                logic = inner_row.get('Logic', '')
                return {
                    "Output Column Name":          output_col,
                    "Original Request Field Name": norm(raw),
                    "Short Description":           short_desc,
                    "Long Description":            '',
                    "Key / Possible Values":       kv,
                    "Derived":                     derived,
                    "Base Field(s)":               ', '.join(fields),
                    "Source Table(s)":             tables,
                    "Logic":                       logic,
                }

    # Choose logic format: structured for window functions, raw snippet otherwise
    is_window = bool(OVER_RE.search(raw) or WITHIN_GROUP_RE.search(raw))
    if derived and is_window:
        logic = window_logic(raw)
    else:
        logic = norm(raw) if derived else ''

    return {
        "Output Column Name":          output_col,
        "Original Request Field Name": norm(raw),
        "Short Description":           short_desc,
        "Long Description":            '',
        "Key / Possible Values":       kv,
        "Derived":                     derived,
        "Base Field(s)":               ', '.join(fields),
        "Source Table(s)":             tables,
        "Logic":                       logic,
    }


# ── Statement processor ───────────────────────────────────────────────────────

def build_derived_col_maps(sql: str) -> dict:
    """
    For every inline-view alias found in sql (i.e.  FROM (SELECT ...) alias ),
    parse that inner SELECT and build a column-level metadata map so that
    outer-query references like  alias.COL  can inherit the precise source
    table, derivation flag, and logic from the inner query.

    Returns:
        { ALIAS_UPPER: { COL_NAME_UPPER: row_dict } }
    where row_dict has the same keys as parse_column() output.
    """
    clean  = strip_literals(strip_comments(sql))
    result = {}

    for content, alias in _subquery_aliases(clean):
        inner_alias_map = all_tables(content)
        inner_raw       = get_raw_select_clause(content)
        if not inner_raw:
            continue
        col_map = {}
        for raw_col in split_columns_raw(inner_raw):
            col_expr = norm(strip_comments(raw_col))
            row = parse_column(col_expr, inner_alias_map)   # no nested map — 1 level
            if row:
                col_map[row['Output Column Name'].upper()] = row
        if col_map:
            result[alias.upper()] = col_map

    return result


def process_statement(stmt: str, verbose: bool = False) -> list:
    parts     = split_union(stmt)

    # Collect all physical tables from EVERY union part, recursively
    alias_map = all_tables(' '.join(parts))

    # Parse each inline-view's SELECT so outer columns inherit source + derivation
    derived_col_map = build_derived_col_maps(stmt)

    if verbose:
        print(f"  [tables] {dict(list(alias_map.items())[:12])}")
        if derived_col_map:
            print(f"  [inner views] {list(derived_col_map.keys())}")

    # Use the ORIGINAL stmt (comments preserved) so -- can become Short Description.
    raw_clause = get_raw_select_clause(stmt)
    if not raw_clause:
        if verbose:
            print("  [warn] No SELECT clause found — statement skipped")
        return []

    rows = []
    for raw_col in split_columns_raw(raw_clause):
        short_desc = extract_inline_comment(raw_col)
        col_expr   = norm(strip_comments(raw_col))
        if verbose:
            _oc, _raw = extract_alias(col_expr)
            print(f"    col: {_oc:<28} derived={is_derived(_raw)}"
                  f"  expr={col_expr[:70]}")
        row = parse_column(col_expr, alias_map, short_desc=short_desc,
                           derived_col_map=derived_col_map)
        if row:
            rows.append(row)
    return rows


# ── File I/O ──────────────────────────────────────────────────────────────────

def read_sql(path: str) -> str:
    with open(path, 'r', encoding='utf-8-sig') as f:
        return f.read()

def write_csv(rows: list, path: str):
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        writer.writeheader()
        writer.writerows(rows)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="SQL Data Dictionary Extractor (Snowflake-compatible)"
    )
    parser.add_argument("input",  help="Path to the SQL file")
    parser.add_argument("output", nargs="?", help="Path to the output CSV (optional)")
    parser.add_argument(
        "--strip-comments",
        action="store_true",
        help="Remove all /* */ and -- comments from the SQL before parsing",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print each column expression and whether it is detected as derived",
    )
    args = parser.parse_args()

    input_path = args.input
    if not os.path.exists(input_path):
        print(f"Error: File not found: {input_path}")
        sys.exit(1)

    if args.output:
        output_path = args.output
    else:
        base        = os.path.splitext(os.path.basename(input_path))[0]
        output_path = os.path.join(
            os.path.dirname(os.path.abspath(input_path)),
            f"{base}_data_dictionary.csv",
        )

    sql_text = read_sql(input_path)
    if args.strip_comments:
        sql_text = strip_comments(sql_text)
        # Save stripped SQL to a new file, then re-read it as the parse source
        base_name    = os.path.splitext(os.path.basename(input_path))[0]
        stripped_path = os.path.join(
            os.path.dirname(os.path.abspath(input_path)),
            f"{base_name}_stripped.sql",
        )
        with open(stripped_path, 'w', encoding='utf-8') as f:
            f.write(sql_text)
        print(f"Stripped SQL saved              : {stripped_path}")
        sql_text = read_sql(stripped_path)

    statements = sqlparse.split(sql_text)

    all_rows, n_select, n_skip = [], 0, 0

    for raw in statements:
        raw = raw.strip()
        if not raw:
            continue
        if is_session_init(raw) or not contains_select(raw):
            n_skip += 1
            continue
        if args.verbose:
            preview = raw.replace('\n', ' ')[:80]
            print(f"\n[stmt] {preview}...")
        rows = process_statement(raw, verbose=args.verbose)
        if rows:
            n_select += 1
            all_rows.extend(rows)

    write_csv(all_rows, output_path)

    print(f"Session-init statements skipped : {n_skip}")
    print(f"SELECT statements processed     : {n_select}")
    print(f"Output columns extracted        : {len(all_rows)}")
    print(f"Saved                           : {output_path}")


if __name__ == '__main__':
    main()
