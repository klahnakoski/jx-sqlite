from jx_base.expressions import SumOp as _SumOp, SqlScript
from jx_sqlite.expressions._utils import SqlScript
from mo_sqlite import sql_call


class SumOp(_SumOp):
    def to_sql(self, schema) -> SqlScript:
        return SqlScript(
            jx_type=self.jx_type, expr=sql_call("SUM", self.term.to_sql(schema).expr), frum=self, schema=schema
        )
