import requests
from bs4 import BeautifulSoup
import re
import yaml
import csv
import os

UNITS_ = 'Units:'
VALID_STATISTICS_ = 'Valid statistics:'
LATENCY_VALUES = ['minimum', 'maximum', 'p50', 'p90', 'p95', 'p99', 'p99.99']
MAPPING_FILE = 'AWS.S3.MAPPING.TEXT'
METRIC_HEADERS = ["metric_name", "metric stats"]
YAML_FILE = "AWS.S3.yaml"
CSV_FILE = "AWS.S3.csv"
csv2 = "AWS.stats.S3.csv"
mn= ["metric_name", "metric_type", "interval", "unit_name", "per_unit_name", "description", "orientation",
                  "integration", "short_name", ]

class AWSS3Extractor:
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
        main_content = soup.find('div', {'id': 'main-content'})
        rows = main_content.findAll('tr')
        metric_name = ''
        self.aws_dict = {'type': 's3', 'keys': []}
        self.aws_list = []
        for row in rows:
            cols = row.findAll('td')
            if cols and len(cols) > 0:
                col = cols[0]
                original_metric_name = col.text.strip()

                metric_name_snake_case = self.snake_case(original_metric_name)
                metric_name = 'aws.s3.' + metric_name_snake_case
                #print(metric_name)

            if cols and len(cols) > 1:
                col = cols[1]
                sections = col.findChildren('p', text=True)
                if sections and len(sections) > 0:
                    idx = 0
                    metric_desc = ''
                    metric_stats = ''
                    metric_units = ''
                    for section in sections:
                        if section:
                            section_string = section.string.strip()
                            if section_string and idx == 0:
                                section_string = " ".join(section_string.split())
                                section_string = section_string.replace('\n', '')
                                metric_desc = section_string
                            elif section_string.startswith(VALID_STATISTICS_):
                                metric_stats = section_string.replace(VALID_STATISTICS_, '').strip()
                                metric_stats = " ".join(metric_stats.split())
                                metric_stats = metric_stats.replace('\n', '')
                                metric_stats = re.sub(r"\([^()]*\)", "", metric_stats).replace(' ', '')



                            elif section_string.startswith(UNITS_):
                                metric_units = section_string.replace(UNITS_, '').strip()
                            if metric_stats == '':
                                metric_stats = 'None'
                        idx = idx + 1

                        if metric_desc and metric_stats and metric_units:
                            metric_units = metric_units.lower()
                            if metric_units != 'count':
                                metric_units = 'gauge'
                            self.add_to_list(self.aws_list, metric_name, metric_units, metric_stats, metric_desc)
                            if metric_stats or metric_name.endswith('latency'):


                                #if metric_name.endswith('latency'):
                                    for suffix in LATENCY_VALUES:
                                        self.add_to_list(self.aws_list, metric_name + '.' + suffix, metric_units,
                                                     metric_stats,
                                                     self.update_description(metric_desc, suffix))
                    self.add_to_list2(self.aws_list2, original_metric_name, metric_stats)
                    self.mapping.append('s3.' + metric_name.replace('aws.s3.','')+' '+
                                             'aws_s3_' + metric_name.replace('aws.s3.',''))

        matchone = soup.findAll('table')
        rowsone = matchone[1].findAll('tr')
        rowsfour = matchone[4].findAll('tr')
        for var in rowsone[1:]:
            colone = var.findAll('td')
            coly = colone[0]
            ogmet = coly.text.strip()
            met_name = 'aws.s3.' + self.snake_case(ogmet)
            #print(met_name)
            colonetwo = colone[1]
            sec = colonetwo.findChildren('p')

            if sec and len(sec) > 0:
                met_desc = ''
                met_units = ''
                met_stats = ''
                idx = 0
                if sec:
                    for v in sec:
                        descriptions = v.findAll(text=True)
                        var1 = ' '.join(descriptions)
                        var1 = var1.replace('\n', '')
                        var2 = ' '.join(var1.split())
                        # print(var2)
                        if var2 and idx == 0:
                            met_desc = var2
                        elif var2.startswith('Valid statistics: '):
                            met_stats = var2.replace('Valid statistics: ','')
                            if '(' in met_stats:
                                met_stats = re.sub(r"\([^()]*\)", "", met_stats).replace(' ', '')


                            #print(met_stats)
                        elif var2.startswith(UNITS_):
                            met_units = var2
                            met_units = met_units.replace(UNITS_, '').strip()
                            if met_units.lower() != 'counts':

                                met_units = 'gauge'
                            else:
                                met_units = 'count'

                        idx = idx + 1
                    self.add_to_list(self.aws_list, metric_name, met_units, met_stats, met_desc, )
                    self.add_to_list(self.aws_list, met_name, met_units, met_stats, met_desc, )
                    self.add_to_list(self.aws_list, met_name, met_units, met_stats, met_desc, )
                    self.add_to_list(self.aws_list, met_name, met_units, met_stats, met_desc, )
                    self.add_to_list2(self.aws_list2, ogmet, met_stats)
                    self.mapping.append('s3.' + met_name.replace('aws.s3.','')+' '
                                             'aws_s3_' +met_name.replace('aws.s3.',''))


        print(self.mapping)
        for t in rowsfour[1:]:
            colo = t.findAll('td')[0]
            yamlmetname = self.snake_case(colo.text.strip())
            ogmetyaml =  colo.text.strip()
            self.aws_dict['keys'].append(
                {'name': yamlmetname, 'alias': 'dimension_' + ogmetyaml})
        print(self.mapping)

    def generate_csv(self):
        os.chdir('CSV_FOLDER')
        with open(CSV_FILE, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(mn)
            writer.writerows(self.aws_list)
        os.chdir('..')

    def generate_csv2(self):
        os.chdir('CSV_METRIC_NAMES')
        with open(csv2, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(METRIC_HEADERS)
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
    def add_to_list(aws_list, metric_name, units, stats, description, ):
        # print(metric_name, '||', units, '||', stats, '||', description, )
        aws_list.append([metric_name, units, "", "", "", description, "", "s3", "", ])

    @staticmethod
    def add_to_list2(aws_list, metric_name, met_stats):
        # print(metric_name, '||', metric_type, "||", description, )
        aws_list.append([metric_name, met_stats])

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
                filehandle.write('\n' +str(listitem))
        os.chdir('..')
if __name__ == "__main__":
    extractor = AWSS3Extractor('https://docs.aws.amazon.com/AmazonS3/latest/userguide/metrics-dimensions.html')
    extractor.load_page()
    extractor.process_content()
    extractor.generate_yaml()
    extractor.generate_csv()
    extractor.generate_csv2()
    extractor.generateMapping()
