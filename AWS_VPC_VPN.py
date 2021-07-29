import requests
from bs4 import BeautifulSoup
import re
import csv
import yaml
import os

res = requests.get('https://docs.aws.amazon.com/vpn/latest/s2svpn/monitoring-cloudwatch-vpn.html')
CSV_FILE = 'AWS.VPC.VPN.csv'
CSV2 = 'AWS.stats.VPC.VPN.csv'
YAML_File = 'AWS.VPC.VPN.yaml'
METRIC_HEADERS= ["metric_name", "metric_type", "interval", "unit_name", "per_unit_name", "description", "orientation","integration", "short_name", ]
mn = ['metric name','metric_stats']
lineadder = ['Minimum', 'Maximum', 'Average']
Statistics = 'Statistics: '
UNITS_ = 'Units:'


class AWSVPC_VPNExtractor:
    def __init__(self, url):
        self.url = url
        self.content = ""
        self.aws_dict = {}
        self.aws_list = []
        self.aws_list2 = []

    def load_page(self):
        page = requests.get(self.url)
        if page.status_code == 200:
            self.content = page.content

    def get_content(self):
        return self.content

    def process_content(self):
        soup = BeautifulSoup(res.text, 'html.parser')
        main_content = soup.find('div', {'id': 'main-content'})
        rows = main_content.findAll('tr')
        metric_name = ''
        self.aws_dict = {'type': 'vpc', 'keys': []}
        self.aws_list = []
        tableone = main_content.findAll('table')[0]
        tabletwo = main_content.findAll('table')[1]
        for i in tableone.findAll('tr')[1:]:
            cols = i.findAll('td')
            col = cols[0]
            original_metric_name = col.text.strip()
            metric_name = 'aws.vpc_'+self.snake_case(original_metric_name)
            cold = cols[1].text.strip()
            metric_desc = cold.replace('\n','').replace('\t','')
            # print(metric_name)
            sections = cols[1].findChildren('p')
            idx = 0
            for z in sections:
                sectstr = z.text.strip()
                descritwo = z.findAll(text=True)
                var10 = ' '.join(descritwo)
                var10 = var10.replace('\n', '')
                sectstr = ' '.join(var10.split())

                if idx == 0 and sectstr and not sectstr.startswith(UNITS_):
                    metric_desc = sectstr.replace('\n','').replace('\t','')
                elif sectstr.startswith(UNITS_):
                    metric_units = sectstr.replace('Units: ','')
                    if metric_units != 'count':
                        metric_units = 'guage'


                idx = idx+1
            self.aws_list2.append([original_metric_name])
            self.aws_list.append([metric_name,metric_units,"","","",metric_desc,"","vpc",""])
        for g in tabletwo.findAll('tr')[1:]:
            colt = g.findAll('td')[0]
            colt = colt.text.strip()
            metricyamlname = colt
            metricnm = 'aws.vpc.'+self.snake_case(colt)
            self.aws_dict['keys'].append(
                {'name': metricnm, 'alias': 'dimension_' + metricyamlname})

    @staticmethod
    def snake_case(input_string):

        if input_string:
            return re.sub(r'(?<!^)(?=[A-Z])', '_', input_string).lower()
        else:
            return input_string

    def generate_csv(self):
        os.chdir('./CSV_FOLDER')

        with open(CSV_FILE, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(METRIC_HEADERS)
            writer.writerows(self.aws_list)
        os.chdir('..')



    def generate_csv2(self):
        os.chdir('./CSV_METRIC_NAMES')


        with open(CSV2, 'w', newline='') as f:

            wr = csv.writer(f)
            wr.writerow(mn)
            wr.writerows(self.aws_list2)
        os.chdir('..')


    def generate_yaml(self):
        os.chdir('./YAML_FOLDER')
        #print(self.aws_dict)
        with open(YAML_File, 'w') as outfile:
         yaml.dump([self.aws_dict], outfile, default_flow_style=False)
        os.chdir('..')



if __name__ == "__main__":
    extractor = AWSVPC_VPNExtractor('https://docs.aws.amazon.com/vpn/latest/s2svpn/monitoring-cloudwatch-vpn.html')
    extractor.load_page()
    extractor.process_content()
    extractor.generate_yaml()
    extractor.generate_csv()
    extractor.generate_csv2()
