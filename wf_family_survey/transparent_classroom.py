import wf_core_data
import pandas as pd
from collections import OrderedDict
import datetime
import re
import logging

logger = logging.getLogger(__name__)

class FamilySurveyTransparentClassroomClient(wf_core_data.TransparentClassroomClient):

    def fetch_family_survey_network_form_template_data(
        self,
        template_name_re
    ):
        network_template_data = self.fetch_network_form_template_data(format='dataframe')
        template_name_re_compiled = re.compile(template_name_re)
        network_template_data['is_family_survey_network_template'] = network_template_data['network_template_name'].apply(
            lambda x: template_name_re_compiled.match(x) is not None
        )
        network_template_data['is_family_survey_network_template'] = network_template_data['is_family_survey_network_template'].astype('bool')
        network_template_data = network_template_data.reindex(columns=[
            'network_template_name',
            'is_family_survey_network_template'
        ])
        network_template_data.sort_values('is_family_survey_network_template', ascending = False, inplace = True)
        return network_template_data
