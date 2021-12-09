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
        template_name_re=None,
        format='dataframe'

    ):
        network_templates_json = self.fetch_network_form_template_data()
        template_name_re_compiled = re.compile(template_name_re)
        network_template_data = []
        for network_template_json in network_templates_json:
            network_template_id = network_template_json.get('id')
            network_template_name = network_template_json.get('name')
            if template_name_re_compiled.match(network_template_name):
                is_family_survey_network_template = True
            else:
                is_family_survey_network_template = False
            network_template_data.append({
                'network_template_id': network_template_id,
                'network_template_name': network_template_name,
                'is_family_survey_network_template': is_family_survey_network_template
            })
        if format == 'dataframe':
            network_template_data = convert_network_template_data_to_df(network_template_data)
        elif format == 'list':
            pass
        else:
            raise ValueError('Data format \'{}\' not recognized'.format(format))
        return network_template_data

def convert_network_template_data_to_df(network_template_data):
    if len(network_template_data) == 0:
        return pd.DataFrame()
    network_template_data_df = pd.DataFrame(
        network_template_data,
        dtype='object'
    )
    network_template_data_df = network_template_data_df.astype({
        'network_template_id': 'int',
        'network_template_name': 'string',
        'is_family_survey_network_template': 'bool'
    })
    network_template_data_df.set_index('network_template_id', inplace=True)
    network_template_data_df.sort_values('is_family_survey_network_template', ascending = False, inplace = True)
    return network_template_data_df
