from typing import Union, Iterable, Dict

from airflow.models import BaseOperator, SkipMixin


class HelloOperator(BaseOperator, SkipMixin):

    def execute(self, context):
        self.logger.info("Hello, World!")
