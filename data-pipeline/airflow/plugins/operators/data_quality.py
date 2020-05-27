from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.dq_checks=dq_checks

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        passed_tests = []
        failed_tests = []
        error_count = 0
        for check in self.dq_checks:
            sql = check.get('check_sql')
            exp_result = check.get('expected_result')
            condition = check.get('condition')
            
            result= redshift.get_records(sql)[0][0]
            
            if condition=='equal':
                if result==exp_result:
                    passed_tests.append(sql)
                else:
                    failed_tests.append(sql)
                    error_count += 1
            elif condition == 'greater_than':
                if result>exp_result:
                    passed_tests.append(sql)
                else:
                    failed_tests.append(sql)
                    error_count += 1
        if error_count != 0:
            self.log.info('Tests failed')
            self.log.info(failing_tests)
            raise ValueError('Data quality check failed')
        else:
            self.log.info('All tests passed')
            self.log.info(passed_tests)
                    
                