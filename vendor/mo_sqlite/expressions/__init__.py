from mo_sqlite.expressions._utils import Sqlite
from mo_sqlite.expressions.select_op import SelectOp
from mo_sqlite.expressions.select_op import SelectOp
from mo_sqlite.expressions.sql_eq_op import SqlEqOp
from mo_sqlite.expressions.sql_inner_join_op import SqlInnerJoinOp
from mo_sqlite.expressions.variable import Variable

Sqlite.register_ops(vars())
