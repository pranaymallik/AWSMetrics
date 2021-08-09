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
YAML_FILE = "AWS.Route53.yaml"
CSV_FILE = "AWS.Route53.csv"
CSV_FILE_2 = "AWS.stats.Route53.csv"
TABLE_IDS = ['w557aac27b7c17b7c11b5b7']


class Route53Runner:
    def __init__(self, url1, url2, url3, url4):
        self.url1 = url1
        self.url2 = url2
        self.url3 = url3
        self.url4 = url4
        self.content1 = ""
        self.content2 = ""
        self.content3 = ""
        self.aws_dict = {}
        self.aws_list = []
        self.aws_metric_names = []
        self.mapping_names = []

    def load_page(self):
        page = requests.get(self.url1)
        if page.status_code == 200:
            self.content = page.content
        page2 = requests.get(self.url2)
        if page2.status_code == 200:
            self.content2 = page2.content
        page3 = requests.get(self.url3)
        if page3.status_code == 200:
            self.content3 = page3.content
        page4 = requests.get(self.url4)
        if page4.status_code == 200:
            self.content4 = page4.content

    def get_content(self):
        return self.content

    def process_content(self):
        # FIRST WEBSITE
        dimensionsx = []
        soup = BeautifulSoup(self.content, 'html.parser')
        tables = soup.findAll('div')
        paragraph1 = soup.find('div', id='main-col-body')
        paragraph1 = paragraph1.findAll('p')
        codes = []
        codes = paragraph1[30].findAll('code')
        for code in codes:
            code = code.string
            if code not in dimensionsx and code != 'AWS/Route53':
                dimensionsx.append(code)
              #  print(code)
        codes = paragraph1[31].findAll('code')
        for code in codes:
            code = code.string
            if code not in dimensionsx and code != 'AWS/Route53':
                dimensionsx.append(code)
             #   print(code)
        self.aws_dict = {'type': 'Route53', 'keys': []}
        self.aws_list = []
        self.aws_metric_names = []
        metricNames = []
        metricDesc = []
        metricStats = []
        metricUnits = []
        rowNames = tables[16].findAll('dt')
        for rowName in rowNames:
            metricNames.append(rowName.string.strip())
        secondPart = tables[16].findAll('dd')
        for p in secondPart:
            ps = p.findAll('p')
            var = ps[0].findAll(text=True)
            var1 = ' '.join(var)
            var1 = var1.replace('\n', '')
            var2 = ' '.join(var1.split())
            metricDesc.append(var2)
            metricStats.append(ps[1].string.replace('Valid statistics: ', '').replace(' (recommended)', ''))
            metricUnits.append(ps[2].string.replace('Units: ', ''))

        # SECOND WEBSITE
        soup2 = BeautifulSoup(self.content2, 'html.parser')
        paragraph1 = soup2.find('div', id='main-col-body')
        paragraph1 = paragraph1.findAll('p')
        codes = []
        codes = paragraph1[32].findAll('code')
        for code in codes:
            code = code.string
            if code not in dimensionsx and code != 'AWS/Route53':
                dimensionsx.append(code)
            #    print(code)

        tables = soup2.findAll('div')
        rowNames2 = tables[13].findAll('dt')
        for rowName2 in rowNames2:
            metricNames.append(rowName2.string.strip())
        secondPart2 = tables[13].findAll('dd')
        for sP2 in secondPart2:
            ps = sP2.findAll('p')
            var = ps[0].findAll(text=True)
            var1 = ' '.join(var)
            var1 = var1.replace('\n', '')
            var2 = ' '.join(var1.split())
            metricDesc.append(var2)
            metricStats.append(ps[1].string.replace('Valid statistics: ', '').replace(' (recommended)', ''))
            metricUnits.append(ps[2].string.replace('Units: ', ''))


        # THIRD WEBSITE
        soup3 = BeautifulSoup(self.content3, 'html.parser')
        paragraph1 = soup3.find('div', id='main-col-body')
        paragraph1 = paragraph1.findAll('p')
        codes = []
        codes = paragraph1[30].findAll('code')
        for code in codes:
            code = code.string
            if code not in dimensionsx and code != 'AWS/Route53':
                dimensionsx.append(code)
               # print(code)

        tables = soup3.findAll('div')
        rowNames2 = tables[17].findAll('dt')
        for rowName2 in rowNames2:
            metricNames.append(rowName2.string.strip())
        secondPart3 = tables[17].findAll('dd')
        for section in secondPart3:
            parts = section.findAll(text=True)
            desc = ''
            for part in parts:
                if 'Valid statistics:' in part:
                    store = part.replace('                                             									', ' ')
                    store = store.replace(VALID_STATISTICS_, '')
                    store = store.replace('\n', '')
                    metricStats.append(store)
                elif UNITS_ in part:
                    metricUnits.append(part.replace('Units: ', ''))
                else:
                    desc = desc + part
                    desc = desc.replace('\n', '')
                    desc = desc.replace('  ', '')
                    desc = desc.replace('\t', '')
            metricDesc.append(desc)


        #FOURTH WEBSITE
        soup4 = BeautifulSoup(self.content4, 'html.parser')
        tables = soup4.findAll('div')

        sections = [tables[17], tables[18], tables[19], tables[20]]
        newvar = []
        for sect in sections:
            variable = sect.findAll('code')
            for v in variable:
                v = v.string
                if ',' not in v and v not in dimensionsx:
                    dimensionsx.append(v)
                 #   print(dimensionsx)
            rowNames4 = sect.find('dt')
            metricNames.append(rowNames4.string.strip())
            secondPart4 = sect.find('dd')
            sP4Chil = secondPart4.findAll(text=True)
            rowDesc = ''
            dimensions = ''
            for sP4C in sP4Chil:
                stuff = sP4C.string
                if 'Valid statistics' in stuff:
                    metricStats.append(stuff.replace('Valid statistics:', ''))
                elif 'Units' in stuff:
                    metricUnits.append(stuff.replace('Unit:', ''))
                elif 'Dimensions' in stuff:
                    dimensions = stuff
                else:
                    # print(stuff)
                    if stuff not in rowDesc:
                        rowDesc = rowDesc + stuff
                        rowDesc = rowDesc.replace('\n', '')
                        rowDesc = rowDesc.replace('  ', '')
                        rowDesc = rowDesc.replace('\t', '')
                        if ').' + dimensions in rowDesc:
                            rowDesc.replace(dimensions, '')
            metricDesc.append(rowDesc)
        i = 0
        for metricName in metricNames:
            self.aws_metric_names.append([metricName, metricStats[i]])
            stringName = metricName
            stringName = stringName.replace('DNS', 'dns')
            stringName = stringName.replace('SEC', 'Sec')
            stringName = stringName.replace('SSL', 'Ssl')
            stringName = stringName.replace('ENI', 'Eni')

            self.add_to_list(self.aws_list, 'aws.route53.' + self.snake_case(stringName), metricUnits[i],
                             metricStats[i], metricDesc[i])
            i += 1
       # print(newvar)
        print(dimensionsx)
        for dd in dimensionsx:
            self.aws_dict['keys'].append( {' name': self.snake_case(dd), 'alias': 'dimension_' + dd})

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
        if [metric_name, units, "", "", "", description, "", "Route53", ""] not in aws_list:
            aws_list.append([metric_name, units, "", "", "", description, "", "Route53", ""])
        #    print(metric_name, '||', units, '||', stats, '||', description)

    @staticmethod
    def snake_case(input_string):
        if input_string:
            return re.sub(r'(?<!^)(?=[A-Z])', '_', input_string).lower()
        else:
            return input_string

    def generate_mapping(self):
        os.chdir('./MAPPING_FOLDER')
        f = open('AWS.Route53.mapping', 'w', newline='')
        for i in self.aws_list:
            string = i[0].replace('aws.','')+" "+i[0].replace('.','_')
            f.write(string)
            f.write('\n')
            self.mapping_names.append([string])
        f.close()
        os.chdir('..')


if __name__ == "__main__":
    extractor = Route53Runner('https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/monitoring-cloudwatch.html',
                              'https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/monitoring-hosted-zones-with-cloudwatch.html',
                              'https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/monitoring-resolver-with-cloudwatch.html',
                              'https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/monitoring-resolver-dns-firewall-with-cloudwatch.html')
    extractor.load_page()
    extractor.process_content()
    extractor.generate_yaml()
    extractor.generate_mapping()
    extractor.generate_csv()
