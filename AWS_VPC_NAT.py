import requests
from bs4 import BeautifulSoup
import re
import csv
import yaml
import os
UNITS_ = 'Units: '
res = requests.get('https://docs.aws.amazon.com/vpc/latest/userguide/vpc-nat-gateway-cloudwatch.html')
CSV_FILE = 'AWS.VPC.NAT.csv'
CSV2 = 'AWS.stats.VPC.NAT.csv'
YAML_File = 'AWS.VPC.NAT.yaml'
METRIC_HEADERS =["metric_name", "metric_type", "interval", "unit_name", "per_unit_name", "description", "orientation",
                  "integration", "short_name",]
mn = ['metric name','stats']
lineadder = ['Minimum', 'Maximum', 'Average']
Statistics = 'Statistics: '


class AWSVPC_NATExtractor:
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
            cold = cols[1]
            sections = cold.findChildren('p')
            if sections and len(sections)>0:
                idx = 0
                metric_stats = ''
                metric_desc = ''
                metric_units= ''
                for p in sections:
                    descriptionssix = p.findAll(text=True)
                    varp = ' '.join(descriptionssix)
                    varp = varp.replace('\n', '')
                    var = ' '.join(varp.split())
                    if idx==0 and not var.startswith('Statistics:') and not var.startswith('Units:') and not var.startswith('Unit:'):
                        metric_desc = var
                    if var.startswith('Units:'):
                        metric_units = var.replace(UNITS_,'')
                        if metric_units == 'Count':
                            metric_units = 'count'
                        else:
                            metric_units = 'guage'
                    elif var.startswith('Unit:'):
                            metric_units = var.replace('Unit: ','')
                            if metric_units == 'Count':
                                metric_units = 'count'
                            else:
                                metric_units = 'guage'

                    if var.startswith('Statistics:'):
                        metric_stats = var
                # print(metric_name)
                self.aws_list2.append([original_metric_name,metric_stats])
                self.add_to_list(self.aws_list,metric_name+'_min',metric_desc,metric_units)
                self.add_to_list(self.aws_list,metric_name+'_max', metric_desc, metric_units)
                self.add_to_list(self.aws_list,metric_name+'_avg', metric_desc, metric_units)
                idx = idx + 1

        for u in main_content.findAll('table')[1].findAll('tr')[1:]:
            coly = u.findAll('td')
            colfq= coly[0]
            original_metric_nametwo = colfq.text.strip()
            # snake case metric name
            metric_nametwo = self.snake_case(original_metric_nametwo)
            # print(metric_nameone)
            colrw = coly[1]
            metric_desctwo = colrw.text.strip()
            self.aws_dict['keys'].append(
                {'name': metric_nametwo , 'alias': 'dimension_' + original_metric_nametwo})

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


    @staticmethod
    def add_to_list(aws_list, metric_name, description,metric_units):
        # print(metric_name, "||", description, )
        aws_list.append([metric_name, metric_units, "", "", "", description, "", "vpc", ""])
if __name__ == "__main__":
    extractor = AWSVPC_NATExtractor('https://docs.aws.amazon.com/vpc/latest/userguide/vpc-nat-gateway-cloudwatch.html')
    extractor.load_page()
    extractor.process_content()
    extractor.generate_yaml()
    extractor.generate_csv()
    extractor.generate_csv2()
