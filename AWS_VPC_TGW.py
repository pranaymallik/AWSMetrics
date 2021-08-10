import requests
from bs4 import BeautifulSoup
import re
import csv
import yaml
import os

res = requests.get('https://docs.aws.amazon.com/vpc/latest/userguide/vpc-nat-gateway-cloudwatch.html')
CSV_FILE = 'AWS.VPC.TGW.csv'
CSV2 = 'AWS.stats.VPC.TGW.csv'
YAML_FILE = 'AWS.VPC.TGW.yaml'
#S = ['metric_name', 'units', 'description', ]
METRIC_HEADERS =  ["metric_name", "metric_type", "interval", "unit_name", "per_unit_name", "description", "orientation","integration","short_name"]
mn = ['metric name', 'stats']
lineadder = ['Minimum', 'Maximum', 'Average']
Statistics = 'Statistics: '
MAPPING_FILE = 'AWS.VPC.TGW.MAPPING.TEXT'


class AWSVPC_TGWExtractor:
    def __init__(self, url):
        self.url = url
        self.content = ""
        self.aws_dict = {}
        self.aws_list = []
        self.aws_list2 = []
        self.mapping = []
    def load_page(self):
        page = requests.get(self.url)
        if page.status_code == 200:
            self.content = page.content

    def get_content(self):
        return self.content

    def process_content(self):
        soup = BeautifulSoup(self.content, 'html.parser')
        tableone = soup.findAll('table')
        rowsone = tableone[0].findAll('tr')
        rowstwo = tableone[1].findAll('tr')
        rowsthree = tableone[2].findAll('tr')
        metric_name = ''
        self.aws_dict = {'type': 'vpc', 'keys': []}
        self.aws_list = []
        for row in rowsone[1:]:
            metric_desc = ''
            metric_statistics = ''
            metric_reporting_criteria = ''
            metric_example = ''
            cols = row.findAll('td')
            col = cols[0]
            allChildren = col.findAll(text=True)
            originalmetricname = col.text.strip().replace('\n', '')
            metric_name = 'aws.vpc.' + self.snake_case(originalmetricname)
            #print(metric_name)
            cold = cols[1]
            results = str(cold.find('code', {'class': "code"}))
            resy = results.replace('<code class="code">blackhole</code>', 'blackhole')
            if resy:
                metric_desc = cold.text.strip().replace('\n','').replace(' a                                                   blackhole route.',' blackhole route')
                #print(metric_name+";",metric_desc)
            self.add_to_list(self.aws_list,metric_name,metric_desc)
            self.add_to_list2(self.aws_list2,originalmetricname,"None")
            self.mapping.append('vpc.' + metric_name.replace('aws.vpc.', '')+' '+
                                 'aws_vpc_' + metric_name.replace('aws.vpc.', ''))

        for j in rowstwo[1:]:
            cole = j.findAll('td')
            co = cole[0]
            ogmet = co.text.strip()
            metric_nameone = 'aws.vpc_'+self.snake_case(ogmet)
            cg = cole[1]
            sed = j.findChildren('p')
            for r in sed:
                descriptionse = r.findAll(text=True)
                var1 = ' '.join(descriptionse)
                var1 = var1.replace('\n', '')
                var2 = ' '.join(var1.split())
            metric_descone = var2
            self.add_to_list(self.aws_list,metric_nameone,metric_descone)
            self.add_to_list2(self.aws_list2,ogmet,"")

        self.mapping.append('vpc.' + metric_nameone.replace('aws.vpc.', '') + ' ' +
                            'aws_vpc_' + metric_nameone.replace('aws.vpc.', ''))






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
            writer = csv.writer(f)
            writer.writerow(mn)
            writer.writerows(self.aws_list2)
        os.chdir('..')


    def generate_yaml(self):
        os.chdir('YAML_FOLDER')
        with open(YAML_FILE, 'w') as outfile:
            yaml.dump([self.aws_dict], outfile, default_flow_style=False)
        os.chdir('..')


    @staticmethod
    def update_description(desc, value):
        desc = desc.replace('per-request', value + ' per-request')
        desc = desc.replace('maximum', value)
        return desc


    @staticmethod
    def add_to_list(aws_list, metric_name, description):
        #print(metric_name, "||", description, )
        aws_list.append([metric_name, "", "", "", "", description, "", "vpc", ""])

    @staticmethod
    def add_to_list2(aws_list, metric_name, description):
        #print(metric_name, "||", description, )
        aws_list.append([metric_name,"None"])


    @staticmethod
    def snake_case(input_string):
        if input_string:
            return re.sub(r'(?<!^)(?=[A-Z])', '_', input_string).lower()
        else:
            return input_string

    def generateMapping(self):
        os.chdir('MAPPING_FOLDER')
        with open(MAPPING_FILE, 'w') as filehandle:
            for listitem in self.mapping:
                filehandle.write(str(listitem)+'\n' )
        os.chdir('..')

if __name__ == "__main__":
    extractor = AWSVPC_TGWExtractor('https://docs.aws.amazon.com/vpc/latest/tgw/transit-gateway-cloudwatch-metrics.html')
    extractor.load_page()
    extractor.process_content()
    extractor.generate_yaml()
    extractor.generate_csv()
    extractor.generate_csv2()
    extractor.generateMapping()