from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow import configuration as conf
from airflow.utils.helpers import parse_template_string

import base64
import logging

class CustomFileTaskHandler(FileTaskHandler):
    def __init__(self, base_log_folder, filename_template):
        super(CustomFileTaskHandler, self).__init__(base_log_folder, filename_template)

    def set_context(self, ti):
        local_loc = self._init_file(ti)
        self.handler = logging.FileHandler(local_loc)
        suffix = conf.get('core','task_log_suffix_template')
        encode = conf.get('core','base64_encoding')
         
        rendered_suffix=""
        if suffix:
            self.suffix_template, self.suffix_jinja_template = parse_template_string(suffix)
            rendered_suffix = self._render_suffix(ti, ti.try_number)
        
        if encode:
            rendered_suffix = base64.b64encode(rendered_suffix.encode()).decode()

        self.handler.setFormatter(logging.Formatter(self.formatter._fmt + ":" + rendered_suffix))
        self.handler.setLevel(self.level)

    def _render_suffix(self, ti, try_number):
        if self.suffix_jinja_template:
            jinja_context = ti.get_template_context()
            jinja_context['try_number'] = try_number
            return self.suffix_jinja_template.render(**jinja_context)

        return self.suffix_template.format(dag_id=ti.dag_id,
                                             task_id=ti.task_id,
                                             execution_date=ti.execution_date.isoformat(),
                                             try_number=try_number)



