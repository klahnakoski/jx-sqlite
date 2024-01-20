
from jx_base.expressions import DefaultOp as _DefaultOp, AndOp
from jx_sqlite.expressions.sql_script import SqlScript
from mo_sql import sql_coalesce


class DefaultOp(_DefaultOp):
    def to_sql(self, schema):
        frum = self.frum.to_sql(schema)
        default = self.default.to_sql(schema)
        return SqlScript(
            jx_type=frum.jx_type | default.jx_type,
            expr=sql_coalesce([frum.expr, default.expr]),
            frum=self,
            miss=AndOp(frum.miss, default.miss),
            schema=schema,
        )
