import requests
from bs4 import BeautifulSoup
import re
import yaml
import csv
import os

UNITS_ = 'Units:'
VALID_STATISTICS_ = 'Valid statistics:'
LATENCY_VALUES = ['minimum', 'maximum', 'p50', 'p90', 'p95', 'p99', 'p99.99']

METRIC_HEADERS = ["metric_name", "metric_type", "interval", "unit_name", "per_unit_name", "description", "orientation",
                  "integration", "short_name"]
YAML_FILE = "AWS.EC2AutoScaling.yaml"
CSV_FILE = "AWS.EC2AutoScaling.csv"
CSV_FOLDER = "CSV_FOLDER"
YAML_FOLDER = "YAML_FOLDER"
CSV_FILE_2 = "AWS.stats.EC2AutoScaling"

class EC2AutoExtractor:
    def __init__(self, url):
        self.url = url
        self.content = ""
        self.aws_dict = {}
        self.aws_list = []
        self.aws_metric_names = []
        self.mapping_names = []

    def load_page(self):
        page = requests.get(self.url)
        if page.status_code == 200:
            self.content = page.content

    def get_content(self):
        return self.content

    def process_content(self):
        soup = BeautifulSoup(self.content, 'html.parser')
        main_content = soup.findAll('div', {'class': 'table-contents'})
        arrMetricName = []
        arrMetricDesc = []
        self.aws_dict = {'type': 'EC2AutoScaling', 'keys': []}

        for main_cont in main_content:
            rows = main_cont.findAll('tr')
            i = 0
            for row in rows:
                if i != 0:
                    var = row.find('td').string
                    arrMetricName.append(var)
                    desc = row.findAll(text=True)
             #       print(desc)
                    var1 = ' '.join(desc)
                    var1 = var1.replace('\n', '')
                    var2 = ' '.join(var1.split())
                    arrMetricDesc.append(var2.replace(var,''))
                i += 1
        count = 0
        for arrMN in arrMetricName:
            self.aws_list.append(
                ["aws.ec2autoscaling." + self.snake_case(arrMN), 'gauge', '','','',arrMetricDesc[count], '', 'AutoScaling',''])
            count += 1
        keysArray = []
        for arrMetricName in arrMetricName:
            self.aws_metric_names.append([arrMetricName,'None'])
            keysArray.append({'name': self.snake_case(arrMetricName), 'alias': 'dimension_' + arrMetricName})
        self.aws_dict = {'type': 'EC2Spot', 'keys': keysArray}


    def generate_csv(self):
        path1 = './CSV_FOLDER'
        os.chdir(path1)
        with open(CSV_FILE, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(METRIC_HEADERS)
            writer.writerows(self.aws_list)
        os.chdir('..')
        path2 = './CSV_METRIC_NAMES'
        os.chdir(path2)
        with open(CSV_FILE_2, 'w', newline='') as f:
        #    print(self.aws_list)
            writer = csv.writer(f)
            writer.writerow(['Metric Name', 'Valid Statistics'])
            writer.writerows(self.aws_metric_names)
        os.chdir('..')



    def generate_yaml(self):
        path1 = './YAML_FOLDER'
        os.chdir(path1)
        with open(YAML_FILE, 'w') as outfile:
            yaml.dump([self.aws_dict], outfile, default_flow_style=False)
        os.chdir('..')


    @staticmethod
    def update_description(desc, value):
        desc = desc.replace('per-request', value + ' per-request')
        desc = desc.replace('maximum', value)
        return desc

    @staticmethod
    def add_to_list(aws_list, metric_name, units, stats, description):
       # print(metric_name, '||', units, '||', stats, '||', description)
        aws_list.append([metric_name, units, "", "", "", description, "", "s3", ""])

    @staticmethod
    def snake_case(input_string):
        if input_string:
            return re.sub(r'(?<!^)(?=[A-Z])', '_', input_string).lower()
        else:
            return input_string

    def generate_mapping(self):
        os.chdir('./MAPPING_FOLDER')
        f = open('AWS.EC2AutoScaling.mapping', 'w', newline='')
        for i in self.aws_list:
            string = i[0].replace('aws.','')+" "+i[0].replace('.','_')
            f.write(string)
            f.write('\n')
            self.mapping_names.append([string])
        f.close()
        os.chdir('..')

if __name__ == "__main__":
    extractor = EC2AutoExtractor('https://docs.aws.amazon.com/autoscaling/ec2/userguide/as-instance-monitoring.html')
    extractor.load_page()
    extractor.process_content()
    extractor.generate_yaml()
    extractor.generate_mapping()
    extractor.generate_csv()
