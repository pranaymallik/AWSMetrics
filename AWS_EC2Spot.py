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
YAML_FILE = "AWS.EC2SPOT.yaml"
CSV_FILE = "AWS.EC2SPOT.csv"
CSV_FILE_2 = "AWS.stats.EC2SPOT.csv"

class EC2SpotExtractor:
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
        main_content = soup.findAll('div', {'class': 'table-contents'})
        body = main_content[0].findAll('tr')
        i = 0
        arrMetricNames = []
        arrMetricDesc = []
        arrMetricUnits = []
        for b in body:
            if i != 0:
                metricName = b.find('td')
                arrMetricNames.append(metricName.string.strip())
                allPs = b.findAll('p')
                description = allPs[0].find_all(text=True)

                var1 = ' '.join(description)
                var1 = var1.replace('\n', '')
                var2 = ' '.join(var1.split())
           #     print(var2)
                arrMetricDesc.append(var2)
                if allPs[1].string.strip() != 'Units: gauge' and allPs[1].string.strip() != 'Units: Count':
                    arrMetricUnits.append('gauge')
                else:
                    arrMetricUnits.append(allPs[1].string.strip().replace('Units: ', ''))
            i += 1
        j = 0
        for metricName in arrMetricNames:
            self.aws_metric_names.append([metricName,'None'])
            self.aws_list.append(['aws.ec2spot.'+self.snake_case(metricName), arrMetricUnits[j],'','','', arrMetricDesc[j],'', "EC2Spot"])
            j += 1
        keysArray = []
        for arrMetricName in arrMetricNames:
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
      #  print(metric_name, '||', units, '||', stats, '||', description)
        aws_list.append([metric_name, units, "", "", "", description, "", "s3", ""])

    @staticmethod
    def snake_case(input_string):
        if input_string:
            return re.sub(r'(?<!^)(?=[A-Z])', '_', input_string).lower()
        else:
            return input_string


if __name__ == "__main__":
    extractor = EC2SpotExtractor(
        'https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/spot-fleet-cloudwatch-metrics.html')
    extractor.load_page()
    extractor.process_content()
    extractor.generate_yaml()
    extractor.generate_csv()
