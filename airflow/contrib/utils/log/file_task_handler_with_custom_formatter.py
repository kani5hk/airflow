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

from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow import configuration as conf
from airflow.utils.helpers import parse_template_string

class FileTaskHandlerWithCustomFormatter(FileTaskHandler):
    """
    FileTaskHandlerWithCustomFormatter is a python log handler that
    extends airflow FileTaskHandler and override set_context method
    to place a custom formmator containing task related information
    this helps in segregation of logs at task level when forwarded
    to central logging system.
    """
    def __init__(self, base_log_folder, filename_template):
        super(FileTaskHandlerWithCustomFormatter, self).__init__(base_log_folder, filename_template)

    def set_context(self, ti):
        local_loc = self._init_file(ti)
        self.handler = logging.FileHandler(local_loc)
        prefix = conf.get('core','task_log_prefix_template')
        
        rendered_prefix=""
        if prefix:
            _, self.prefix_jinja_template = parse_template_string(prefix)
            rendered_prefix = self._render_prefix(ti, ti.try_number)

        self.handler.setFormatter(logging.Formatter(rendered_prefix + ":" + self.formatter._fmt))
        self.handler.setLevel(self.level)

    def _render_prefix(self, ti, try_number):
        if self.prefix_jinja_template:
            jinja_context = ti.get_template_context()
            jinja_context['try_number'] = try_number
            return self.prefix_jinja_template.render(**jinja_context)
        logging.warning("'task_log_prefix_template' is in invalid format, ignoring the variable value")
        return ""
