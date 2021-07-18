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
YAML_FILE = "AWS.lamba.yaml"
CSV_FILE = "AWS.lamba.csv"


class LambdaExtractor:
    def __init__(self, url):
        self.url = url
        self.content = ""
        self.aws_dict = {}
        self.aws_list = []

    def load_page(self):
        page = requests.get(self.url)
        if page.status_code == 200:
            self.content = page.content

    def get_content(self):
        return self.content

    def process_content(self):
        soup = BeautifulSoup(self.content, 'html.parser')
        main_content = soup.findAll('div', {'class': 'itemizedlist'})
        arrTitles = []
        arrMetricType = []
        arrMetricDesc = []
        arrIntegration = []
        #  rows = main_content.findAll({'ul'})
        for i in range(1, 4):
            rows = main_content[i].find_all('li')
            for row in rows:
                titles = row.find_all('code')
                description = row.find_all(text=True)
                var1 = ' '.join(description)
                var1 = var1.replace('\n', '')
                var2 = ' '.join(var1.split())
                var1index = var2.find('–') + 2
                description1 = var2[var1index:]
                # print(description1)
                arrMetricDesc.append(var2)
                for title in titles:
                    title = title.string.strip()
                    if title not in arrTitles and title != '.5':
                        arrTitles.append(title)
                        arrMetricType.append("gauge")
                        arrIntegration.append("lambda")
        metric_name = ''
        self.aws_dict = {'type': 'Lambda', 'keys': []}
        self.aws_list = []
        i = 0
        for arrTitle in arrTitles:
            newArr = ["aws.lamba." + arrTitle, arrMetricType[i], '', '', '', arrMetricDesc[i], '', arrIntegration[i],'']
            self.aws_list.append(newArr)
            i += 1
        keysArray = []
        for arrMetricName in arrTitles:
            keysArray.append({'name': self.snake_case(arrMetricName), 'alias': 'dimension_' + arrMetricName})
        self.aws_dict = {'type': 'EC2Spot', 'keys': keysArray}

    def generate_csv(self):
        path1 = 'C:\\Users\Pranay\PycharmProjects\AWSMetrics/CSV_FOLDER'
        os.chdir(path1)
        with open(CSV_FILE, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(METRIC_HEADERS)
            writer.writerows(self.aws_list)

    def generate_yaml(self):
        path1 = 'C:\\Users\Pranay\PycharmProjects\AWSMetrics/YAML_FOLDER'
        os.chdir(path1)
        with open(YAML_FILE, 'w') as outfile:
            yaml.dump([self.aws_dict], outfile, default_flow_style=False)

    @staticmethod
    def update_description(desc, value):
        desc = desc.replace('per-request', value + ' per-request')
        desc = desc.replace('maximum', value)
        return desc

    @staticmethod
    def add_to_list(aws_list, metric_name, units, stats, description):
        #print(metric_name, '||', units, '||', stats, '||', description)
        aws_list.append([metric_name, units, "", "", "", description, "", "s3", ""])

    @staticmethod
    def snake_case(input_string):
        if input_string:
            return re.sub(r'(?<!^)(?=[A-Z])', '_', input_string).lower()
        else:
            return input_string


if __name__ == "__main__":
    extractor = LambdaExtractor('https://docs.aws.amazon.com/lambda/latest/dg/monitoring-metrics.html')
    extractor.load_page()
    extractor.process_content()
    extractor.generate_yaml()
    extractor.generate_csv()
