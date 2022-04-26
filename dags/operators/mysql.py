from typing import Any, Callable, Dict, Iterable, Mapping, Optional, Union

from airflow.models import BaseOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.decorators import apply_defaults


class MySqlReadOperator(BaseOperator):
    """
    Executes sql code and returns the resulting records or row. in a specific MySQL database

    :param sql: the sql code to be executed. Can receive a str representing a
        sql statement, a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
        (templated)
    :type sql: str or list[str]
    :param mysql_conn_id: reference to a specific mysql database
    :type mysql_conn_id: str
    :param parameters: (optional) the parameters to render the SQL query with.
    :type parameters: dict or iterable
    :param database: name of database which overwrite defined one in connection
    :type database: str
    :param records_filter: A function allowing you to manipulate the records.
        e.g records_filter=lambda records: records[0][0].
        The callable takes the records object as the first positional argument
    :type records_filter: A lambda or defined function.
    :param first: if True, returns the first resulting records. not a set of records.
        (default value: False)
    :type database: bool
    """

    template_fields = ("sql",)
    template_ext = (".sql",)
    ui_color = "#d4d4d4"

    @apply_defaults
    def __init__(
        self,
        *,
        sql: str,
        mysql_conn_id: str = "mysql_default",
        parameters: Optional[Union[Mapping, Iterable]] = None,
        database: Optional[str] = None,
        records_filter: Optional[Callable[..., Any]] = None,
        first: Optional[bool] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.mysql_conn_id = mysql_conn_id
        self.sql = sql
        self.parameters = parameters
        self.database = database
        self.first = first
        self.records_filter = records_filter

    def execute(self, context: Dict) -> None:
        self.log.info("Executing: %s", self.sql)
        hook = MySqlHook(mysql_conn_id=self.mysql_conn_id, schema=self.database)

        if self.first:
            records = hook.get_first(self.sql, parameters=self.parameters)
        else:
            records = hook.get_records(self.sql, parameters=self.parameters)

        if self.records_filter:
            return self.records_filter(records)
        return records
