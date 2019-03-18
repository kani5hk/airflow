# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import logging
import logging.config
import os
import unittest
import six

from airflow.models import TaskInstance, DAG, DagRun
from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.timezone import datetime
from airflow.utils.log.logging_mixin import set_context
from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow.utils.db import create_session
from airflow.utils.state import State
from airflow import configuration as conf

DEFAULT_DATE = datetime(2019, 1, 1)
TASK_LOGGER = 'airflow.task'
FILE_TASK_HANDLER = 'task'
TASK_HANDLER_CLASS = 'airflow.contrib.utils.log.file_task_handler_with_custom_formatter.FileTaskHandlerWithCustomFormatter'

class FileTaskHandlerWithCustomFormatter(unittest.TestCase):
    def setUp(self):
        super(FileTaskHandlerWithCustomFormatter, self).setUp()
        DEFAULT_LOGGING_CONFIG['handlers']['task']['class'] = TASK_HANDLER_CLASS
        conf.set('core','task_log_prefix_template',"{{ti.dag_id}}-{{ti.task_id}}")

        logging.config.dictConfig(DEFAULT_LOGGING_CONFIG)
        logging.root.disabled = False
        #self.cleanUp()

    def test_formatter(self):
        dag = DAG('dag_for_testing_formatter', start_date=DEFAULT_DATE)
        task = DummyOperator(task_id='task_for_testing_formatter', dag=dag)
        ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)

        logger = ti.log
        ti.log.disabled = False
        file_handler = next((handler for handler in logger.handlers
            if handler.name == FILE_TASK_HANDLER), None)
        self.assertIsNotNone(file_handler)

        expected_formatter_value = "dag_for_testing_formatter-task_for_testing_formatter:"+file_handler.formatter._fmt      
        set_context(logger, ti)
        self.assertEqual(expected_formatter_value,file_handler.handler.formatter._fmt)
