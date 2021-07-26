import requests
from bs4 import BeautifulSoup
import re
import yaml
import csv
import os

UNITS_ = 'Units:'
VALID_STATISTICS_ = 'Valid Statistics:'
LATENCY_VALUES = ['minimum', 'maximum', 'p50', 'p90', 'p95', 'p99', 'p99.99']

METRIC_HEADERS = ["metric_name", "metric_type", "interval", "unit_name", "per_unit_name", "description", "orientation",
                  "integration", "short_name"]
YAML_FILE = "AWS.DynamoDB.yaml"
CSV_FILE = "AWS.DynamoDB.csv"
CSV_FILE_2 = "AWS.stats.DynamoDB.csv"
TABLE_IDS = ['w557aac27b7c17b7c11b5b7']


class DynamoDB:
    def __init__(self, url):
        self.url = url
        self.content = ""
        self.aws_dict = {}
        self.aws_list = []
        self.aws_metric_names = []

    def load_page(self):
        page = requests.get(self.url)
        if page.status_code == 200:
            self.content = page.content

    def get_content(self):
        return self.content

    def process_content(self):
        soup = BeautifulSoup(self.content, 'html.parser')
        tables = soup.findAll('table')
        metric_name = ''
        self.aws_dict = {'type': 'Kinesis', 'keys': []}
        self.aws_list = []
        for table in tables:
            if table is not None:
                tableID = table.get('id')
                if tableID in TABLE_IDS:
                    num = 1
                else:
                    continue
            rows = table.findAll('tr')
            for row in rows:
                rowStats = []
                cols = row.findAll('td')
                if cols and len(cols) > 0:
                    col = cols[0]
                    original_metric_name = col.text.strip()
                    #print(original_metric_name)
                    metric_name_snake_case = self.snake_case(original_metric_name.replace('.', ''))
                    metric_name = 'aws.DynamoDB.' + metric_name_snake_case

                    self.aws_dict['keys'].append(
                        {'name': metric_name_snake_case, 'alias': 'dimension_' + original_metric_name})
                if cols and len(cols) > 1:
                    col = cols[1]
                    sections = col.findChildren('p')
                    if sections and len(sections) > 0:
                        idx = 0
                        metric_desc = ''
                        metric_stats = ''
                        metric_units = ''
                        i = 0;
                        for section in sections:
                            if section:
                                section_string = section.text.strip()
                                if section_string and idx == 0:
                                    section_string = " ".join(section_string.split())
                                    section_string = section_string.replace('\n', '')
                                    metric_desc = section_string
                            #        print(metric_desc)
                                elif section_string.startswith(VALID_STATISTICS_):
                                    listOfDimensions = col.find('ul')
                                    listItems = listOfDimensions.findAll('li')
                                    for li in listItems:
                                        listChildren = li.find('code')
                                        rowStats.append(listChildren.string.strip())

                                    metric_stats = section_string.replace(VALID_STATISTICS_, '').strip()
                                    metric_stats = " ".join(metric_stats.split())
                                    metric_stats = metric_stats.replace('\n', '')
                                elif section_string.startswith(UNITS_):
                                    metric_units = section_string.replace(UNITS_, '').strip()
                            idx = idx + 1
                            if metric_desc and metric_units:
                                metric_units = metric_units.lower()
                                if metric_units != 'count':
                                    metric_units = 'gauge'
                                self.add_to_list(self.aws_list, metric_name, metric_units, metric_stats, metric_desc)
                         #       print(self.aws_list, metric_name, metric_units, metric_stats, metric_desc)
                                if metric_name.endswith('latency'):
                                    for suffix in LATENCY_VALUES:
                                        self.add_to_list(self.aws_list, metric_name + '.' + suffix, metric_units,
                                                         metric_stats,
                                                         self.update_description(metric_desc, suffix))
                            i+=1;
                    rowStats1 = '\n'.join(rowStats)
                    self.aws_metric_names.append([original_metric_name, rowStats1])
                    #print(self.aws_metric_names)

    def generate_csv(self):
        path1 = './CSV_FOLDER'
        os.chdir(path1)
        with open(CSV_FILE, 'w', newline='') as f:
        #    print(self.aws_list)
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
        if [metric_name, units, "", "", "", description, "", "DynamoDB", ""]  not in aws_list:
            aws_list.append([metric_name, units, "", "", "", description, "", "DynamoDB", ""])
        #    print(metric_name, '||', units, '||', stats, '||', description)

    @staticmethod
    def snake_case(input_string):
        if input_string:
            return re.sub(r'(?<!^)(?=[A-Z])', '_', input_string).lower()
        else:
            return input_string


if __name__ == "__main__":
    extractor = DynamoDB('https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/metrics-dimensions.html')
    extractor.load_page()
    extractor.process_content()
    extractor.generate_yaml()
    extractor.generate_csv()