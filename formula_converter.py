"""
Alteryx formula to Trino SQL converter.

Converts Alteryx Designer formula functions to their Trino SQL equivalents.
Reference: https://help.alteryx.com/current/en/designer/functions.html

Target Platform: Starburst (Trino-based)
SQL Dialect: Trino SQL
"""
import re
from typing import Dict, Tuple, Optional, Callable


# Alteryx function to Trino SQL mapping
# Format: 'AlteryxFunction': ('TrinoEquivalent', arg_count or -1 for variable)
ALTERYX_TO_TRINO_FUNCTIONS: Dict[str, Tuple[str, int]] = {
    # ============================================================
    # STRING FUNCTIONS
    # ============================================================
    'Trim': ('TRIM', 1),
    'TrimLeft': ('LTRIM', 1),
    'TrimRight': ('RTRIM', 1),
    'Left': ('SUBSTR({0}, 1, {1})', 2),  # Left(str, n) -> SUBSTR(str, 1, n)
    'Right': ('SUBSTR({0}, LENGTH({0}) - {1} + 1)', 2),  # Right(str, n) -> SUBSTR(str, LEN-n+1)
    'Substring': ('SUBSTR', 3),  # Substring(str, start, len) -> SUBSTR(str, start, len)
    'Length': ('LENGTH', 1),
    'UpperCase': ('UPPER', 1),
    'LowerCase': ('LOWER', 1),
    'Proper': ('INITCAP', 1),  # Proper case (capitalize first letter of each word)
    'PadLeft': ('LPAD', 3),  # PadLeft(str, len, char) -> LPAD(str, len, char)
    'PadRight': ('RPAD', 3),  # PadRight(str, len, char) -> RPAD(str, len, char)
    'Replace': ('REPLACE', 3),  # Replace(str, old, new) -> REPLACE(str, old, new)
    'ReplaceFirst': ('REGEXP_REPLACE({0}, {1}, {2}, 1)', 3),  # Replace first occurrence only
    'Concat': ('CONCAT', -1),  # Variable args: Concat(a, b, ...) -> CONCAT(a, b, ...)
    'ReverseString': ('REVERSE', 1),
    'Contains': ('STRPOS({0}, {1}) > 0', 2),  # Contains(str, sub) -> STRPOS > 0
    'StartsWith': ('STARTS_WITH', 2),  # StartsWith(str, prefix) -> STARTS_WITH(str, prefix)
    'EndsWith': ('ENDS_WITH', 2),  # EndsWith(str, suffix) -> ENDS_WITH(str, suffix)
    'FindString': ('STRPOS', 2),  # FindString(str, sub) -> STRPOS(str, sub)
    'CountWords': ('CARDINALITY(SPLIT({0}, \' \'))', 1),  # Count words by splitting on space
    'GetWord': ('SPLIT({0}, \' \')[{1}]', 2),  # GetWord(str, n) -> SPLIT(str, ' ')[n]
    'MD5_ASCII': ('MD5', 1),  # MD5 hash
    'MD5_UNICODE': ('MD5', 1),  # MD5 hash (same in Trino)

    # ============================================================
    # MATH FUNCTIONS
    # ============================================================
    'Abs': ('ABS', 1),
    'Ceil': ('CEIL', 1),
    'Ceiling': ('CEIL', 1),  # Alias
    'Floor': ('FLOOR', 1),
    'Round': ('ROUND', 2),  # Round(value, decimals) -> ROUND(value, decimals)
    'Pow': ('POWER', 2),  # Pow(base, exp) -> POWER(base, exp)
    'Power': ('POWER', 2),  # Alias
    'Sqrt': ('SQRT', 1),
    'Exp': ('EXP', 1),
    'Log': ('LN', 1),  # Natural log
    'Log10': ('LOG10', 1),
    'Log2': ('LOG2', 1),
    'Mod': ('MOD', 2),  # Mod(a, b) -> MOD(a, b)
    'Sign': ('SIGN', 1),
    'Sin': ('SIN', 1),
    'Cos': ('COS', 1),
    'Tan': ('TAN', 1),
    'ASin': ('ASIN', 1),
    'ACos': ('ACOS', 1),
    'ATan': ('ATAN', 1),
    'ATan2': ('ATAN2', 2),
    'Rand': ('RAND()', 0),  # Random number
    'RandInt': ('FLOOR(RAND() * ({1} - {0} + 1)) + {0}', 2),  # RandInt(min, max)
    'PI': ('PI()', 0),

    # ============================================================
    # MIN/MAX FUNCTIONS
    # ============================================================
    'Min': ('LEAST', -1),  # Min(a, b, ...) -> LEAST(a, b, ...)
    'Max': ('GREATEST', -1),  # Max(a, b, ...) -> GREATEST(a, b, ...)

    # ============================================================
    # CONVERSION FUNCTIONS
    # ============================================================
    'ToNumber': ('CAST({0} AS DOUBLE)', 1),  # ToNumber(str) -> CAST(str AS DOUBLE)
    'ToInteger': ('CAST({0} AS BIGINT)', 1),  # ToInteger(str) -> CAST(str AS BIGINT)
    'ToString': ('CAST({0} AS VARCHAR)', 1),  # ToString(val) -> CAST(val AS VARCHAR)
    'CharToInt': ('CODEPOINT', 1),  # Character to ASCII code
    'IntToHex': ('TO_HEX(CAST({0} AS VARBINARY))', 1),  # Integer to hex
    'HexToNumber': ('FROM_HEX({0})', 1),  # Hex to number
    'ConvertToDate': ('CAST({0} AS DATE)', 1),
    'ConvertToDateTime': ('CAST({0} AS TIMESTAMP)', 1),
    'BinToInt': ('FROM_BASE({0}, 2)', 1),  # Binary string to integer

    # ============================================================
    # DATE/TIME FUNCTIONS
    # ============================================================
    'DateTimeNow': ('CURRENT_TIMESTAMP', 0),
    'DateTimeToday': ('CURRENT_DATE', 0),
    'DateTimeYear': ('YEAR', 1),  # Year(dt) -> YEAR(dt)
    'DateTimeMonth': ('MONTH', 1),  # Month(dt) -> MONTH(dt)
    'DateTimeDay': ('DAY', 1),  # Day(dt) -> DAY(dt)
    'DateTimeHour': ('HOUR', 1),  # Hour(dt) -> HOUR(dt)
    'DateTimeMinutes': ('MINUTE', 1),  # Minute(dt) -> MINUTE(dt)
    'DateTimeSeconds': ('SECOND', 1),  # Second(dt) -> SECOND(dt)
    'DateTimeDayOfWeek': ('DAY_OF_WEEK', 1),  # DayOfWeek(dt) -> DAY_OF_WEEK(dt)
    'DateTimeDayOfYear': ('DAY_OF_YEAR', 1),
    'DateTimeQuarter': ('QUARTER', 1),
    'DateTimeWeekOfYear': ('WEEK', 1),

    # Date arithmetic
    'DateTimeAdd': ('DATE_ADD({2}, {1}, {0})', 3),  # DateTimeAdd(dt, n, unit) -> DATE_ADD(unit, n, dt)
    'DateTimeDiff': ('DATE_DIFF({2}, {0}, {1})', 3),  # DateTimeDiff(dt1, dt2, unit) -> DATE_DIFF(unit, dt1, dt2)

    # Date formatting/parsing (requires special handling)
    'DateTimeFormat': ('DATE_FORMAT({0}, {1})', 2),  # DateTimeFormat(dt, fmt) -> DATE_FORMAT(dt, fmt)
    'DateTimeParse': ('DATE_PARSE({0}, {1})', 2),  # DateTimeParse(str, fmt) -> DATE_PARSE(str, fmt)

    # Date truncation
    'DateTimeTrim': ('DATE_TRUNC({1}, {0})', 2),  # DateTimeTrim(dt, unit) -> DATE_TRUNC(unit, dt)
    'DateTimeFirstOfMonth': ('DATE_TRUNC(\'month\', {0})', 1),
    'DateTimeLastOfMonth': ('LAST_DAY_OF_MONTH({0})', 1),

    # ============================================================
    # CONDITIONAL/LOGICAL FUNCTIONS
    # ============================================================
    'IIF': ('CASE WHEN {0} THEN {1} ELSE {2} END', 3),  # IIF(cond, true, false) -> CASE WHEN
    'IsNull': ('({0} IS NULL)', 1),
    'IsEmpty': ('({0} = \'\')', 1),
    'IsNumber': ('TRY_CAST({0} AS DOUBLE) IS NOT NULL', 1),
    'IsString': ('TYPEOF({0}) = \'varchar\'', 1),
    'Null': ('NULL', 0),
    'Coalesce': ('COALESCE', -1),  # Coalesce(a, b, ...) -> COALESCE(a, b, ...)
    'NullIf': ('NULLIF', 2),  # NullIf(a, b) -> NULLIF(a, b)
    'Switch': (None, -1),  # Special handling required

    # ============================================================
    # REGEX FUNCTIONS
    # ============================================================
    'REGEX_Match': ('REGEXP_LIKE', 2),  # REGEX_Match(str, pattern) -> REGEXP_LIKE(str, pattern)
    'REGEX_Replace': ('REGEXP_REPLACE', 3),  # REGEX_Replace(str, pattern, repl) -> REGEXP_REPLACE
    'REGEX_CountMatches': ('CARDINALITY(REGEXP_EXTRACT_ALL({0}, {1}))', 2),

    # ============================================================
    # SPATIAL FUNCTIONS (Trino has limited spatial support)
    # ============================================================
    'Distance': ('ST_DISTANCE', 2),  # Requires Trino spatial functions
    'Centroid': ('ST_CENTROID', 1),

    # ============================================================
    # FINANCE FUNCTIONS (implemented as expressions)
    # ============================================================
    'Average': ('AVG', -1),  # As aggregation or expression

    # ============================================================
    # FILE FUNCTIONS (limited in SQL context)
    # ============================================================
    'FileExists': (None, 1),  # Not directly translatable to SQL
    'FileGetDir': (None, 1),  # Not directly translatable
    'FileGetExt': ('REGEXP_EXTRACT({0}, \'\\.([^.]+)$\', 1)', 1),  # Extract file extension
    'FileGetName': ('REGEXP_EXTRACT({0}, \'([^/\\\\\\\\]+)$\', 1)', 1),  # Extract filename
}

# Alteryx date format specifiers to Trino format specifiers
ALTERYX_DATE_FORMAT_MAP = {
    '%Y': '%Y',      # 4-digit year
    '%y': '%y',      # 2-digit year
    '%m': '%m',      # Month (01-12)
    '%d': '%d',      # Day (01-31)
    '%H': '%H',      # Hour (00-23)
    '%M': '%i',      # Minute (00-59) - Trino uses %i
    '%S': '%s',      # Second (00-59) - Trino uses %s
    '%B': '%M',      # Full month name
    '%b': '%b',      # Abbreviated month name
    '%A': '%W',      # Full weekday name
    '%a': '%a',      # Abbreviated weekday name
    '%p': '%p',      # AM/PM
    '%I': '%I',      # Hour (01-12)
}


class FormulaConverter:
    """Converts Alteryx formulas to Trino SQL."""

    def __init__(self):
        self.function_map = ALTERYX_TO_TRINO_FUNCTIONS
        self.date_format_map = ALTERYX_DATE_FORMAT_MAP
        self._conversion_notes: list = []
        # Create case-insensitive lookup for function names
        self._func_name_map = {k.lower(): k for k in self.function_map}

    def convert(self, expr: str) -> str:
        """
        Convert an Alteryx expression to Trino SQL.

        Args:
            expr: Alteryx formula expression

        Returns:
            Trino SQL expression
        """
        if not expr:
            return "NULL"

        self._conversion_notes = []
        sql = expr.strip()

        # Replace Alteryx field references [FieldName] with "FieldName"
        sql = re.sub(r'\[([^\]]+)\]', r'"\1"', sql)

        # Replace operators
        sql = sql.replace('==', '=')
        sql = sql.replace('&&', ' AND ')
        sql = sql.replace('||', ' OR ')
        sql = sql.replace('!=', '<>')
        sql = sql.replace('!', ' NOT ')

        # Convert functions
        sql = self._convert_all_functions(sql)

        return sql

    def _convert_all_functions(self, expr: str) -> str:
        """Convert all Alteryx functions in an expression to Trino SQL."""
        result = expr
        converted_positions = set()  # Track positions we've already converted

        # Process functions from innermost to outermost
        max_iterations = 100
        iteration = 0

        while iteration < max_iterations:
            # Find all function matches and their positions
            matches = []
            for alteryx_func in self.function_map:
                pattern = rf'\b{re.escape(alteryx_func)}\s*\('
                for match in re.finditer(pattern, result, re.IGNORECASE):
                    start_idx = match.start()
                    paren_start = match.end() - 1
                    paren_end = self._find_matching_paren(result, paren_start)
                    if paren_end != -1:
                        # Get the actual function name from the match
                        actual_func_name = result[start_idx:paren_start].strip()

                        # Skip if this is an already-converted function (all caps Trino function)
                        # Alteryx functions use mixed case, Trino uses all caps
                        if actual_func_name.isupper() and actual_func_name != alteryx_func:
                            continue

                        # Check if this function has no nested Alteryx functions
                        args_str = result[paren_start + 1:paren_end]
                        has_nested = any(
                            re.search(rf'\b{re.escape(f)}\s*\(', args_str, re.IGNORECASE)
                            for f in self.function_map
                            if not result[result.find(f, paren_start + 1):result.find(f, paren_start + 1) + len(f) + 1].rstrip('(').isupper()
                        )
                        matches.append((start_idx, paren_start, paren_end, alteryx_func, has_nested, actual_func_name))

            if not matches:
                break

            # Process innermost functions first (those without nested functions)
            # If all have nested functions, process the rightmost (deepest) one
            innermost = [m for m in matches if not m[4]]
            if innermost:
                # Pick the first innermost function
                start_idx, paren_start, paren_end, alteryx_func, _, actual_name = innermost[0]
            else:
                # Pick the rightmost function (likely deepest nested)
                matches.sort(key=lambda x: x[0], reverse=True)
                start_idx, paren_start, paren_end, alteryx_func, _, actual_name = matches[0]

            # Extract and parse arguments
            args_str = result[paren_start + 1:paren_end]
            args = self._split_args(args_str)

            # Convert to Trino
            trino_expr = self._convert_function(alteryx_func, args)
            if trino_expr:
                old_result = result
                result = result[:start_idx] + trino_expr + result[paren_end + 1:]
                # If no change was made, we're stuck in a loop
                if result == old_result:
                    break
            else:
                # Skip this function if conversion fails
                break

            iteration += 1

        return result

    def _convert_function(self, func_name: str, args: list) -> Optional[str]:
        """Convert a single Alteryx function to Trino SQL."""
        # Case-insensitive lookup
        canonical_name = self._func_name_map.get(func_name.lower())
        if not canonical_name:
            self._conversion_notes.append(
                f"No mapping for Alteryx function: {func_name}"
            )
            return None

        mapping = self.function_map.get(canonical_name)
        if not mapping:
            return None

        trino_template, expected_args = mapping

        if trino_template is None:
            # Function requires special handling
            return self._handle_special_function(func_name, args)

        if expected_args == 0:
            # Zero-arg function
            return trino_template

        if expected_args == -1:
            # Variable args - use as-is with same function name
            return f"{trino_template}({', '.join(args)})"

        if '{' in trino_template:
            # Template with placeholders
            try:
                return trino_template.format(*args)
            except (IndexError, KeyError):
                self._conversion_notes.append(
                    f"Argument mismatch for {func_name}: expected {expected_args}, got {len(args)}"
                )
                return f"/* TODO: Fix {func_name} */ {func_name}({', '.join(args)})"
        else:
            # Simple function name mapping
            return f"{trino_template}({', '.join(args)})"

    def _handle_special_function(self, func_name: str, args: list) -> str:
        """Handle functions that need special conversion logic."""
        if func_name.lower() == 'switch':
            # Switch(Value, Default, Case1, Result1, ..., CaseN, ResultN)
            # -> CASE Value WHEN Case1 THEN Result1 ... ELSE Default END
            if len(args) < 2:
                return f"/* Invalid Switch */ NULL"

            value = args[0]
            default = args[1]
            cases = args[2:]

            if len(cases) % 2 != 0:
                self._conversion_notes.append(
                    "Switch function has odd number of case/result pairs"
                )
                return f"/* TODO: Fix Switch */ COALESCE({', '.join(args)})"

            case_stmts = []
            for i in range(0, len(cases), 2):
                case_val = cases[i]
                result = cases[i + 1]
                case_stmts.append(f"WHEN {case_val} THEN {result}")

            return f"CASE {value} {' '.join(case_stmts)} ELSE {default} END"

        return f"/* TODO: Convert {func_name} */ {func_name}({', '.join(args)})"

    def _find_matching_paren(self, expr: str, start: int) -> int:
        """Find the index of the closing parenthesis matching the one at start."""
        depth = 1
        i = start + 1
        in_string = False
        string_char = None

        while i < len(expr) and depth > 0:
            char = expr[i]

            if char in ('"', "'") and not in_string:
                in_string = True
                string_char = char
            elif char == string_char and in_string:
                in_string = False
                string_char = None
            elif not in_string:
                if char == '(':
                    depth += 1
                elif char == ')':
                    depth -= 1

            i += 1

        return i - 1 if depth == 0 else -1

    def _split_args(self, args_str: str) -> list:
        """Split function arguments respecting nested parentheses and quotes."""
        args = []
        current = ""
        depth = 0
        in_string = False
        string_char = None

        for char in args_str:
            if char in ('"', "'") and not in_string:
                in_string = True
                string_char = char
                current += char
            elif char == string_char and in_string:
                in_string = False
                string_char = None
                current += char
            elif char == '(' and not in_string:
                depth += 1
                current += char
            elif char == ')' and not in_string:
                depth -= 1
                current += char
            elif char == ',' and depth == 0 and not in_string:
                args.append(current.strip())
                current = ""
            else:
                current += char

        if current.strip():
            args.append(current.strip())

        return args

    def convert_date_format(self, alteryx_format: str) -> str:
        """Convert Alteryx date format string to Trino format string."""
        result = alteryx_format
        for alteryx_spec, trino_spec in self.date_format_map.items():
            result = result.replace(alteryx_spec, trino_spec)
        return result

    def get_conversion_notes(self) -> list:
        """Get any notes or warnings from the last conversion."""
        return self._conversion_notes


def convert_alteryx_expression(expr: str) -> str:
    """
    Convenience function to convert an Alteryx expression to Trino SQL.

    Args:
        expr: Alteryx formula expression

    Returns:
        Trino SQL expression
    """
    converter = FormulaConverter()
    return converter.convert(expr)


# Mapping of Alteryx aggregation functions for Summarize tool
ALTERYX_AGGREGATION_TO_TRINO = {
    'Sum': 'SUM',
    'Count': 'COUNT',
    'CountDistinct': 'COUNT(DISTINCT {0})',
    'CountNonNull': 'COUNT',
    'CountNull': 'SUM(CASE WHEN {0} IS NULL THEN 1 ELSE 0 END)',
    'CountBlank': 'SUM(CASE WHEN {0} = \'\' THEN 1 ELSE 0 END)',
    'Min': 'MIN',
    'Max': 'MAX',
    'Avg': 'AVG',
    'First': 'FIRST_VALUE({0}) OVER ()',
    'Last': 'LAST_VALUE({0}) OVER ()',
    'Concat': 'LISTAGG({0}, \',\')',  # String concatenation
    'Mode': 'APPROX_MOST_FREQUENT({0}, 1)',  # Approximate mode
    'StdDev': 'STDDEV',
    'StdDevP': 'STDDEV_POP',
    'Variance': 'VARIANCE',
    'VarianceP': 'VAR_POP',
    'Median': 'APPROX_PERCENTILE({0}, 0.5)',  # Approximate median
    'Percentile': 'APPROX_PERCENTILE({0}, {1})',  # Percentile
}


def convert_aggregation(alteryx_agg: str, field: str, *extra_args) -> str:
    """
    Convert an Alteryx aggregation function to Trino SQL.

    Args:
        alteryx_agg: Alteryx aggregation type
        field: Field to aggregate
        extra_args: Additional arguments (e.g., percentile value)

    Returns:
        Trino SQL aggregation expression
    """
    mapping = ALTERYX_AGGREGATION_TO_TRINO.get(alteryx_agg)
    if not mapping:
        return f"/* TODO: {alteryx_agg} */ {field}"

    if '{' in mapping:
        # Template with placeholders
        args = [field] + list(extra_args)
        try:
            return mapping.format(*args)
        except (IndexError, KeyError):
            return f"{mapping.split('(')[0]}({field})"
    else:
        return f"{mapping}({field})"
