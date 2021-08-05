from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        operators.S3ToRedshiftOperator,
        operators.StageToRedshiftOperator,
        operators.DataQualityOperator,
        operators.S3UploadObjectOperator,
        operators.S3DeleteObjectsOperator,
        operators.LoadDataOperator
    ]
    helpers = [
        helpers.SqlQueries
    ]
