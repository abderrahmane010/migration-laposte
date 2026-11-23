"""Dataiku formula language -> Spark SQL (ROADMAP phase 4, task 2).

The formula is parsed with a ``lark`` grammar, then every node is translated into a
typed sqlglot expression. A construct absent from this module (function, operator,
unknown column or variable, syntax) raises ``UnsupportedConstruct``: never guessed.

ASSUMPTION (H-44): syntax of the Dataiku formula language: literals (numbers, "..." or
'...' strings, true, false), columns by bare name or ``val("name")``/``strval``/
``numval``, project variables ``${name}``, operators ``+ - * / %``, comparisons
``== != < <= > >=``, booleans ``&& || !``, function calls ``f(x, ...)`` and the
object notation ``x.f(...)`` (same as ``f(x, ...)``).

Known semantic traps (pieges_semantiques.md), each tested on PySpark:
- ``+`` concatenates as soon as one side is a string;
- ``round`` rounds half up, negatives included (``round(-3.5) = -3``);
- ``isBlank`` and ``coalesce`` treat '' as empty, not only NULL;
- a division by zero gives NULL (``try_divide``) instead of failing in ANSI mode.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from lark import Lark, Token, Transformer, v_args
from lark.exceptions import LarkError, VisitError
from sqlglot import exp

from dku2dbx.errors import UnsupportedConstruct
from dku2dbx.translate.sql import column

GRAMMAR = r"""
?start: expr
?expr: or_
?or_: and_ | or_ "||" and_ -> or_op
?and_: not_ | and_ "&&" not_ -> and_op
?not_: comparison | "!" not_ -> not_op
?comparison: sum | sum COMP_OP sum -> compare
?sum: product | sum ADD_OP product -> arith
?product: unary | product MUL_OP unary -> arith
?unary: postfix | "-" unary -> neg
?postfix: atom | postfix "." NAME "(" [args] ")" -> method
?atom: NUMBER -> number
     | STRING -> string
     | TRUE -> true
     | FALSE -> false
     | NAME "(" [args] ")" -> call
     | NAME -> column_ref
     | VARIABLE -> variable
     | "(" expr ")"
args: expr ("," expr)*

TRUE.2: "true"
FALSE.2: "false"
VARIABLE: /\$\{[A-Za-z0-9_.]+\}/
NAME: /[A-Za-z_][A-Za-z0-9_]*/
NUMBER: /\d+(\.\d+)?/
STRING: /"(\\.|[^"\\])*"/ | /'(\\.|[^'\\])*'/
COMP_OP: "==" | "!=" | "<=" | ">=" | "<" | ">"
ADD_OP: "+" | "-"
MUL_OP: "*" | "/" | "%"
%import common.WS
%ignore WS
"""

_PARSER = Lark(GRAMMAR, parser="lalr", maybe_placeholders=False)

STRING, NUMBER, BOOLEAN, DATE = "string", "number", "boolean", "date"
NUMERIC_DKU_TYPES = frozenset({"tinyint", "smallint", "int", "bigint", "float", "double"})


def dku_kind(dku_type: str | None) -> str | None:
    """Formula type of a Dataiku column type (None: not usable in a formula yet)."""
    if dku_type == "string":
        return STRING
    if dku_type in NUMERIC_DKU_TYPES:
        return NUMBER
    if dku_type == "boolean":
        return BOOLEAN
    if dku_type == "date":
        return DATE
    return None


@dataclass(frozen=True)
class Typed:
    """A translated sub-expression and its formula type."""

    sql: exp.Expr
    kind: str


@dataclass(frozen=True)
class JobParameter:
    """A project variable read as a job parameter (``:name``) instead of its value.

    Set by the bundle generator (phase 6) in the project variables; the value is
    the default of the parameter and gives its type.
    """

    name: str
    value: Any


@dataclass(frozen=True)
class Context:
    recipe: str
    columns: Mapping[str, str | None]
    """Column name -> Dataiku type, as available at this Prepare step."""
    variables: Mapping[str, Any]


def _unsupported(ctx: Context, detail: str) -> UnsupportedConstruct:
    return UnsupportedConstruct(ctx.recipe, "formula", detail)


def _text(value: Typed) -> exp.Expr:
    return (
        value.sql
        if value.kind == STRING
        else exp.Cast(this=value.sql, to=exp.DataType.build("STRING"))
    )


def _blank_to_null(value: Typed) -> exp.Expr:
    """'' counts as empty in Dataiku: NULLIF(x, '') for strings."""
    if value.kind != STRING:
        return value.sql
    return exp.Nullif(this=value.sql, expression=exp.Literal.string(""))


def _require(ctx: Context, name: str, args: list[Typed], kinds: list[str]) -> None:
    if len(args) != len(kinds):
        raise _unsupported(ctx, f"{name}() expects {len(kinds)} argument(s), got {len(args)}")
    for i, (arg, kind) in enumerate(zip(args, kinds, strict=True)):
        if kind not in ("any", arg.kind):
            raise _unsupported(ctx, f"{name}() argument {i + 1} must be {kind}, got {arg.kind}")


def _round(ctx: Context, args: list[Typed]) -> Typed:
    # Half up, negatives included (Java Math.round): round(-3.5) = -3, round(2.5) = 3.
    if len(args) not in (1, 2) or any(a.kind != NUMBER for a in args):
        raise _unsupported(ctx, "round() expects 1 or 2 numbers")
    value = exp.paren(args[0].sql)
    if len(args) == 1:
        return Typed(
            exp.Floor(this=exp.Add(this=value, expression=exp.Literal.number(0.5))), NUMBER
        )
    if not isinstance(args[1].sql, exp.Literal):
        raise _unsupported(ctx, "round() digits must be a literal")
    scale = exp.Pow(this=exp.Literal.number(10), expression=args[1].sql)
    shifted = exp.Add(
        this=exp.Mul(this=value, expression=scale), expression=exp.Literal.number(0.5)
    )
    return Typed(exp.Div(this=exp.Floor(this=shifted), expression=scale.copy()), NUMBER)


def _if(ctx: Context, args: list[Typed]) -> Typed:
    if len(args) not in (2, 3) or args[0].kind != BOOLEAN:
        raise _unsupported(ctx, "if() expects a condition and 1 or 2 values")
    values = args[1:]
    kinds = {v.kind for v in values}
    if len(kinds) != 1:
        raise _unsupported(ctx, f"if() branches have different types {sorted(kinds)}")
    case = exp.Case(ifs=[exp.If(this=args[0].sql, true=values[0].sql)])
    if len(values) == 2:
        case.set("default", values[1].sql)
    return Typed(case, values[0].kind)


def _is_blank(ctx: Context, args: list[Typed]) -> Typed:
    _require(ctx, "isBlank", args, ["any"])
    return Typed(exp.Is(this=exp.paren(_blank_to_null(args[0])), expression=exp.Null()), BOOLEAN)


def _is_non_blank(ctx: Context, args: list[Typed]) -> Typed:
    return Typed(exp.Not(this=exp.paren(_is_blank(ctx, args).sql)), BOOLEAN)


def _coalesce(ctx: Context, args: list[Typed]) -> Typed:
    kinds = {a.kind for a in args}
    if len(args) < 2 or len(kinds) != 1:
        raise _unsupported(ctx, "coalesce() expects 2+ values of one type")
    return Typed(
        exp.Coalesce(
            this=_blank_to_null(args[0]), expressions=[_blank_to_null(a) for a in args[1:]]
        ),
        args[0].kind,
    )


def _string_fn(fn: Callable[[exp.Expr], exp.Expr]) -> Callable[[Context, list[Typed]], Typed]:
    def handler(ctx: Context, args: list[Typed]) -> Typed:
        _require(ctx, "string function", args, [STRING])
        return Typed(fn(args[0].sql), STRING)

    return handler


def _number_fn(fn: Callable[[exp.Expr], exp.Expr]) -> Callable[[Context, list[Typed]], Typed]:
    def handler(ctx: Context, args: list[Typed]) -> Typed:
        _require(ctx, "math function", args, [NUMBER])
        return Typed(fn(args[0].sql), NUMBER)

    return handler


def _length(ctx: Context, args: list[Typed]) -> Typed:
    _require(ctx, "length", args, [STRING])
    return Typed(exp.Length(this=args[0].sql), NUMBER)


def _substring(ctx: Context, args: list[Typed]) -> Typed:
    # ASSUMPTION (H-45): substring(s, from[, to]) with 0-based from and exclusive to.
    if len(args) not in (2, 3) or args[0].kind != STRING or any(a.kind != NUMBER for a in args[1:]):
        raise _unsupported(ctx, "substring() expects a string and 1 or 2 numbers")
    start = exp.Add(this=args[1].sql, expression=exp.Literal.number(1))
    if len(args) == 2:
        return Typed(exp.Substring(this=args[0].sql, start=start), STRING)
    length = exp.Sub(this=args[2].sql, expression=args[1].sql.copy())
    return Typed(exp.Substring(this=args[0].sql, start=start, length=length), STRING)


def _bool_string(
    fn: Callable[[exp.Expr, exp.Expr], exp.Expr],
) -> Callable[[Context, list[Typed]], Typed]:
    def handler(ctx: Context, args: list[Typed]) -> Typed:
        _require(ctx, "string test", args, [STRING, STRING])
        return Typed(fn(args[0].sql, args[1].sql), BOOLEAN)

    return handler


def _min_max(fn: type[exp.Func]) -> Callable[[Context, list[Typed]], Typed]:
    def handler(ctx: Context, args: list[Typed]) -> Typed:
        if len(args) < 2 or any(a.kind != NUMBER for a in args):
            raise _unsupported(ctx, "min()/max() expect 2+ numbers")
        return Typed(fn(this=args[0].sql, expressions=[a.sql for a in args[1:]]), NUMBER)

    return handler


# Dataiku function name -> handler. Only functions with a verified or documented
# meaning are listed; any other name raises UnsupportedConstruct.
FUNCTIONS: dict[str, Callable[[Context, list[Typed]], Typed]] = {
    "if": _if,
    "isBlank": _is_blank,
    "isNonBlank": _is_non_blank,
    "coalesce": _coalesce,
    "toLowercase": _string_fn(lambda s: exp.Lower(this=s)),
    "toUppercase": _string_fn(lambda s: exp.Upper(this=s)),
    "trim": _string_fn(lambda s: exp.Trim(this=s)),
    "strip": _string_fn(lambda s: exp.Trim(this=s)),
    "length": _length,
    "substring": _substring,
    "startsWith": _bool_string(lambda s, p: exp.StartsWith(this=s, expression=p)),
    "endsWith": _bool_string(lambda s, p: exp.func("endswith", s, p)),
    "contains": _bool_string(lambda s, p: exp.Contains(this=s, expression=p)),
    "round": _round,
    "floor": _number_fn(lambda x: exp.Floor(this=x)),
    "ceil": _number_fn(lambda x: exp.Ceil(this=x)),
    "abs": _number_fn(lambda x: exp.Abs(this=x)),
    "sqrt": _number_fn(lambda x: exp.Sqrt(this=x)),
    "min": _min_max(exp.Least),
    "max": _min_max(exp.Greatest),
}
COLUMN_ACCESSORS = frozenset({"val", "strval", "numval"})
_COMPARISONS: dict[str, type[exp.Binary]] = {
    "==": exp.EQ,
    "!=": exp.NEQ,
    "<": exp.LT,
    "<=": exp.LTE,
    ">": exp.GT,
    ">=": exp.GTE,
}


@v_args(inline=True)
class _Translator(Transformer[Token, Typed]):
    def __init__(self, ctx: Context) -> None:
        super().__init__()
        self.ctx = ctx

    # -- literals ---------------------------------------------------------- #
    def number(self, token: Token) -> Typed:
        text = str(token)
        literal = exp.Literal.number(text)
        if "." in text:  # Dataiku numbers are doubles: no exact DECIMAL arithmetic
            return Typed(exp.Cast(this=literal, to=exp.DataType.build("DOUBLE")), NUMBER)
        return Typed(literal, NUMBER)

    def string(self, token: Token) -> Typed:
        raw = str(token)[1:-1]
        value = raw.encode("utf-8").decode("unicode_escape") if "\\" in raw else raw
        return Typed(exp.Literal.string(value), STRING)

    def true(self, _: Token) -> Typed:
        return Typed(exp.true(), BOOLEAN)

    def false(self, _: Token) -> Typed:
        return Typed(exp.false(), BOOLEAN)

    # -- references -------------------------------------------------------- #
    def _column(self, name: str) -> Typed:
        if name not in self.ctx.columns:
            raise _unsupported(self.ctx, f"column '{name}' is not available at this step")
        kind = dku_kind(self.ctx.columns[name])
        if kind is None:
            raise _unsupported(
                self.ctx, f"column '{name}' has type {self.ctx.columns[name]!r}, not mapped"
            )
        return Typed(column(name), kind)

    def column_ref(self, token: Token) -> Typed:
        return self._column(str(token))

    def variable(self, token: Token) -> Typed:
        name = str(token)[2:-1]
        if name not in self.ctx.variables:
            raise _unsupported(self.ctx, f"variable '${{{name}}}' cannot be resolved")
        value = self.ctx.variables[name]
        if isinstance(value, JobParameter):
            return self._parameter(name, value)
        if isinstance(value, bool):
            return Typed(exp.true() if value else exp.false(), BOOLEAN)
        if isinstance(value, (int, float)):
            return Typed(exp.Literal.number(value), NUMBER)
        if isinstance(value, str):
            return Typed(exp.Literal.string(value), STRING)
        raise _unsupported(self.ctx, f"variable '${{{name}}}' is not a scalar")

    def _parameter(self, name: str, param: JobParameter) -> Typed:
        # Job parameters arrive as text: an explicit CAST gives the variable its type.
        marker = exp.Placeholder(this=param.name)
        value = param.value
        if isinstance(value, bool):
            return Typed(exp.Cast(this=marker, to=exp.DataType.build("BOOLEAN")), BOOLEAN)
        if isinstance(value, int):
            return Typed(exp.Cast(this=marker, to=exp.DataType.build("BIGINT")), NUMBER)
        if isinstance(value, float):
            return Typed(exp.Cast(this=marker, to=exp.DataType.build("DOUBLE")), NUMBER)
        if isinstance(value, str):
            return Typed(marker, STRING)
        raise _unsupported(self.ctx, f"variable '${{{name}}}' is not a scalar")

    # -- calls ------------------------------------------------------------- #
    def args(self, *items: Typed) -> list[Typed]:
        return list(items)

    def call(self, name: Token, arguments: list[Typed] | None = None) -> Typed:
        fname, args = str(name), arguments or []
        if fname in COLUMN_ACCESSORS:
            return self._accessor(fname, args)
        handler = FUNCTIONS.get(fname)
        if handler is None:
            raise _unsupported(self.ctx, f"function '{fname}' is not mapped")
        return handler(self.ctx, args)

    def method(self, target: Typed, name: Token, arguments: list[Typed] | None = None) -> Typed:
        return self.call(name, [target, *(arguments or [])])

    def _accessor(self, fname: str, args: list[Typed]) -> Typed:
        if len(args) != 1 or not isinstance(args[0].sql, exp.Literal) or args[0].kind != STRING:
            raise _unsupported(self.ctx, f"{fname}() expects a literal column name")
        ref = self._column(str(args[0].sql.this))
        if fname == "strval":
            return Typed(_text(ref), STRING)
        if fname == "numval":
            return Typed(exp.Cast(this=ref.sql, to=exp.DataType.build("DOUBLE")), NUMBER)
        return ref

    # -- operators --------------------------------------------------------- #
    def arith(self, left: Typed, op: Token, right: Typed) -> Typed:
        symbol = str(op)
        if symbol == "+" and STRING in (left.kind, right.kind):
            # Dataiku "+" concatenates as soon as one side is a string.
            # ASSUMPTION (H-46): a NULL side gives NULL, as in SQL (to verify on Dataiku).
            return Typed(exp.DPipe(this=_text(left), expression=_text(right), safe=True), STRING)
        if left.kind != NUMBER or right.kind != NUMBER:
            raise _unsupported(self.ctx, f"'{symbol}' between {left.kind} and {right.kind}")
        if symbol == "/":
            return Typed(exp.func("try_divide", left.sql, right.sql), NUMBER)
        node = {"+": exp.Add, "-": exp.Sub, "*": exp.Mul, "%": exp.Mod}[symbol]
        return Typed(node(this=exp.paren(left.sql), expression=exp.paren(right.sql)), NUMBER)

    def neg(self, value: Typed) -> Typed:
        if value.kind != NUMBER:
            raise _unsupported(self.ctx, f"'-' on {value.kind}")
        return Typed(exp.Neg(this=exp.paren(value.sql)), NUMBER)

    def compare(self, left: Typed, op: Token, right: Typed) -> Typed:
        if left.kind != right.kind:
            raise _unsupported(self.ctx, f"comparison between {left.kind} and {right.kind}")
        node = _COMPARISONS[str(op)]
        return Typed(node(this=exp.paren(left.sql), expression=exp.paren(right.sql)), BOOLEAN)

    def _boolean(self, *values: Typed) -> None:
        if any(v.kind != BOOLEAN for v in values):
            raise _unsupported(self.ctx, "boolean operator on a non-boolean value")

    def and_op(self, left: Typed, right: Typed) -> Typed:
        self._boolean(left, right)
        return Typed(exp.and_(exp.paren(left.sql), exp.paren(right.sql)), BOOLEAN)

    def or_op(self, left: Typed, right: Typed) -> Typed:
        self._boolean(left, right)
        return Typed(exp.or_(exp.paren(left.sql), exp.paren(right.sql)), BOOLEAN)

    def not_op(self, value: Typed) -> Typed:
        self._boolean(value)
        return Typed(exp.Not(this=exp.paren(value.sql)), BOOLEAN)


def translate_formula(text: str, ctx: Context) -> Typed:
    """The typed Spark SQL expression of a Dataiku formula."""
    try:
        tree = _PARSER.parse(text)
    except LarkError as exc:
        raise _unsupported(ctx, f"syntax not mapped: {type(exc).__name__}") from None
    try:
        result = _Translator(ctx).transform(tree)
    except VisitError as exc:
        if isinstance(exc.orig_exc, UnsupportedConstruct):
            raise exc.orig_exc from None
        raise
    if not isinstance(result, Typed):
        raise _unsupported(ctx, "empty formula")
    return result
