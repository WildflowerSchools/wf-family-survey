import wf_core_data
import pandas as pd
import numpy as np
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
        network_form_template_data = self.fetch_network_form_template_data(format='dataframe')
        template_name_re_compiled = re.compile(template_name_re)
        network_form_template_data['is_family_survey_template'] = network_form_template_data['form_template_name'].apply(
            lambda x: template_name_re_compiled.match(x) is not None
        )
        network_form_template_data['is_family_survey_template'] = network_form_template_data['is_family_survey_template'].astype('bool')
        network_form_template_data = network_form_template_data.reindex(columns=[
            'form_template_name',
            'is_family_survey_template'
        ])
        network_form_template_data.sort_values('is_family_survey_template', ascending = False, inplace = True)
        return network_form_template_data

    def fetch_family_survey_school_form_template_data(
        self,
        family_survey_network_form_template_ids,
        school_ids=None
    ):
        form_template_data = self.fetch_form_template_data(
            school_ids=school_ids,
            format='dataframe'
        )
        form_template_data['is_family_survey_template'] = form_template_data['widgets'].apply(
            lambda widgets: np.any([
                (widget.get('type') == 'EmbeddedForm') and (int(widget.get('embedded_form_id')) in family_survey_network_form_template_ids)
                for widget in widgets
            ])
        )
        form_template_data['is_family_survey_template'] = form_template_data['is_family_survey_template'].astype('bool')
        form_template_data = form_template_data.reindex(columns=[
            'form_template_name',
            'is_family_survey_template'
        ])
        form_template_data.sort_values('is_family_survey_template', ascending = False, inplace = True)
        return form_template_data
