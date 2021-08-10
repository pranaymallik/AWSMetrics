import requests
from bs4 import BeautifulSoup
import re
import yaml
import csv
import os

UNITS_ = 'Units:'
VALID_STATISTICS_ = 'Valid statistics:'
LATENCY_VALUES = ['minimum', 'maximum', 'p50', 'p90', 'p95', 'p99', 'p99.99']
MAPPING_FILE = 'AWS.RDS.MAPPING.TEXT'

METRIC_HEADERS = ["metric_name", "metric_type", "interval", "unit_name", "per_unit_name", "description", "orientation",
                  "integration", "short_name", ]
mn = ["Metric Name","metric stats"]
YAML_FILE = "AWS.RDS.yaml"
CSV_FILE = "AWS.RDS.csv"
CSV2 = "AWS.stats.RDS.csv"

CODE_MAP = {
    'EBSByteBalance%': 'ebs_byte_balance%',
    'ReadIOPS': 'read_iops',
    'CPUUtilization': 'cpu_utilization',
    'EBSIOBalance%': 'ebs_io_balance%',
    'FailedSQLServerAgentJobsCount': 'failed_sql_server_agent_jobs_count',
    'WriteIOPS': 'write_iops',
    'CPUCreditBalance': 'cpu_credit_balance',
    'CPUCreditUsage': 'cpu_credit_usage',
    'MaximumUsedTransactionIDs': 'maximum_used_transaction_ids'
}

class AWSRDSExtractor:
    def __init__(self, url):
        self.url = url
        self.content = ""
        self.aws_dict = {}
        self.aws_list = []
        self.aws_listtwo = []
        self.mapping = []

    def load_page(self):
        page = requests.get(self.url)
        if page.status_code == 200:
            self.content = page.content

    def get_content(self):
        return self.content

    def process_content(self):
        soup = BeautifulSoup(self.content, 'html.parser')
        main_content = soup.find('div', {'id': 'main-content'})
        rows = main_content.findAll('tr')
        mate = soup.findAll('table')
        mat = mate[1].findAll('tr')
        metric_name = ''
        metric_units = ''
        metric_desc = ''
        metric_console_name = ''
        self.aws_dict = {'type': 'rds', 'keys': []}
        self.aws_dictone = {'type': 'rds', 'keys': []}
        self.aws_list = []
        for row in rows:
            cols = row.findAll('td')
            if cols and len(cols) == 4:
                col = cols[0]
                original_metric_name = col.text.strip()
                self.add_to_list2(self.aws_listtwo,original_metric_name)
               # print(original_metric_name)
                metric_name_snake_case = self.snake_case(original_metric_name)
                if original_metric_name in CODE_MAP.keys():
                    metric_name_snake_case = CODE_MAP[original_metric_name]
                metric_name = 'aws.rds.' + metric_name_snake_case
                self.aws_dict['keys'].append(
                    {'name': metric_name_snake_case, 'alias': 'dimension_' + original_metric_name})
                colsconsole = cols[1]
                if 'instances' not in colsconsole:
                    metric_console_name = colsconsole.text.strip()
                colsdesc = cols[2].findChildren('p')
                #metric_desc = colsdesc.text.strip().replace('\n','').replace('\t','')
                for sect in colsdesc:
                    descriptions = sect.findAll(text=True)
                    var1 = ' '.join(descriptions)
                    var1 = var1.replace('\n', '')
                    var2 = ' '.join(var1.split())
                metric_desc = var2


                metric_units = cols[3].text.strip()
                if 'Count' not in metric_units:
                    metric_units = 'guage'
                if '/' in metric_units:
                    metric_units = 'guage'
                else:
                    metric_units = 'count'
                self.add_to_list(self.aws_list, metric_name, metric_units, metric_desc)
                self.mapping.append('rds.' + metric_name.replace('aws.rds.', '')+' '+
                                     'aws_rds_' + metric_name.replace('aws.rds.', ''))
        #print(self.mapping)
        for h in mat[1:]:
            colv = h.findAll('td')
            codv = colv[0].find('code')
           # print(codv.text.strip())
            '''self.aws_dict['keys'].append(
                {'name': metseven, 'alias': 'dimension_' + co.text.strip()})'''

    def generate_csv(self):
        os.chdir('CSV_FOLDER')
        with open(CSV_FILE, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(METRIC_HEADERS)
            writer.writerows(self.aws_list)
        os.chdir('..')


    def generate_csv2(self):
        os.chdir('CSV_METRIC_NAMES')
        with open(CSV2, 'w', newline='') as f:
            write = csv.writer(f)
            write.writerow(mn)
            write.writerows(self.aws_listtwo)
        os.chdir('..')

    def generateMapping(self):
        os.chdir('MAPPING_FOLDER')
        with open(MAPPING_FILE, 'w') as filehandle:
            for listitem in self.mapping:
                filehandle.write(str(listitem)+'\n')
        os.chdir('..')

    def generate_yaml(self):
        os.chdir('YAML_FOLDER')
        with open(YAML_FILE, 'w') as outfile:
            yaml.dump([self.aws_dict], outfile, default_flow_style=False)
        os.chdir('..')

    @staticmethod
    def add_to_list(aws_list, metric_name, metric_type, description, ):
        #print(metric_name, '||', metric_type, "||", description, )
        aws_list.append([metric_name, metric_type, "", "", "", description, "", "rds", ""])

    @staticmethod
    def add_to_list2(aws_list, metric_name,):
        # print(metric_name, '||', metric_type, "||", description, )
        aws_list.append([metric_name,"None"])

    @staticmethod
    def snake_case(input_string):
        if input_string:
            return re.sub(r'(?<!^)(?=[A-Z])', '_', input_string).lower()
        else:
            return input_string


if __name__ == "__main__":
    extractor = AWSRDSExtractor('https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/monitoring-cloudwatch.html')
    extractor.load_page()
    extractor.process_content()
    extractor.generate_yaml()
    extractor.generate_csv()
    extractor.generate_csv2()
    extractor.generateMapping()