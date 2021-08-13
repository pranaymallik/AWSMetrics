import requests
from bs4 import BeautifulSoup
import re
import yaml
import csv
import os
import numpy as np

UNITS_ = 'Units:'
VALID_STATISTICS_ = 'Valid statistics:'
LATENCY_VALUES = ['minimum', 'maximum', 'p50', 'p90', 'p95', 'p99', 'p99.99']
MAPPING_FILE = 'AWS.ELB.MAPPING.TEXT'
METRIC_HEADERS = ["metric_name", "metric Stats"]
YAML_FILE = "AWS.ELB.yaml"
CSV_FILE = "AWS.ELB.csv"
CSV2 = "AWS.stats.ELB.csv"
mn =  ["metric_name", "metric_type", "interval", "unit_name", "per_unit_name", "description", "orientation",
                  "integration", "short_name", ]
httpcode2 = 'aws.elb.httpcode_backend_2xx'
CODE_MAP = {
    'DesyncMitigationMode_NonCompliant_Request_Count': 'desync_mitigation_mode_non_compliant_request_count',
    'aws.elbdesync_mitigation_mode__non_compliant__request__count': 'aws.elbdesync_mitigation_mode_non_compliant_request_count',
    'HTTPCode_ELB_4xx': 'http_code_elb_4xx',
    'HTTPCode_ELB_5xx': 'http_code_elb_xx',
    'EstimatedALBActiveConnectionCount': 'aws.elb.estimated_alb_active_connection_count',
    'EstimatedALBConsumedLCUs': 'aws.elb.estimated_alb_consumed_lcus',
    'EstimatedALBNewConnectionCount': 'aws.elb.estimated_alb_new_connection_count',
    'HTTPCode_Backend_2XX,                                                   HTTPCode_Backend_3XX,                                                   HTTPCode_Backend_4XX,                                                   HTTPCode_Backend_5XX'
    : 'http_code_backend_2xx_http_code_backend_3xx_http_code_backend_4xx_http_code_backend_5xx'

}


class ELBExtractor:
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
        SUM_ = ''
        MIN_ = ''
        MAX_ = ''
        AVG_ = ''
        soup = BeautifulSoup(self.content, 'html.parser')
        tableone = soup.find('table', id="w282aac21b7c13b5")
        rowsone = tableone.findAll('tr')
        metric_name = ''
        self.aws_dict = {'type': 'elb', 'keys': []}
        self.aws_list = []
        self.mapping = []
        htppcode2 = 'http_code_backend_2xx'
        for row in rowsone[1:]:
            metric_desc = ''
            metric_statistics = ''
            metric_reporting_criteria = ''
            metric_example = ''
            metric_ststone = ''

            cols = row.findAll('td')
            col = cols[0]

            allChildren = col.findAll(text=True)
            originalmetricname = col.text.strip().replace('\n', '')
            metric_name = 'aws.elb.' + self.snake_case(originalmetricname)
            originalmetricname = originalmetricname.replace('\t', '')
            if originalmetricname =='HTTPCode_Backend_2XX,                                                   HTTPCode_Backend_3XX,                                                   HTTPCode_Backend_4XX,                                                   HTTPCode_Backend_5XX':
                self.aws_list2.append(['HTTPCode_Backend_2XX',"None"])
                self.aws_list2.append(['HTTPCode_Backend_3XX','None'])
                self.aws_list2.append(['HTTPCode_Backend_4XX','None'])
                self.aws_list2.append(['HTTPCode_Backend_5XX','None'])



            else:
                self.aws_list2.append([originalmetricname,'None'])


            if allChildren and len(allChildren)> 1 :
                #print(allChildren[2])
                var11 = ' '.join(allChildren)
                var11 = var11.replace('\n', '')
                var22 = ' '.join(var11.split())
                firstComma = var22.find(',')
                secondComma = var22.find(',', firstComma + 2)
                thirdComma = var22.find(',', secondComma + 2)
                firstString = var22[0:firstComma]
                secondString = var22[firstComma:secondComma]
                thirdString = var22[secondComma:thirdComma]
                fourthString = var22[thirdComma:]
                secondString = secondString.replace(', ','')
                thirdString = thirdString.replace(', ','')
                fourthString = fourthString.replace(', ','')
                firstString = secondString.replace(' ','')
                secondString = thirdString.replace(' ','')
                thirdString = fourthString.replace(' ','')

                stringsArray = ['aws.elb.'+firstString.lower() ,'aws.elb.'+secondString.lower(),'aws.elb.'+thirdString.lower() ,'aws.elb.'+fourthString.lower()]
                for op in stringsArray:
                    sections = colone.findChildren('p')
                    if sections and len(sections) > 0:
                        idx = 0
                        for sect in sections:
                            descriptions = sect.findAll(text=True)
                            var1 = ' '.join(descriptions)
                            var1 = var1.replace('\n', '')
                            var2 = ' '.join(var1.split())
                            if var2 and idx == 0:
                                metric_desc = var2

                                if 'Sum' in metric_desc:
                                    SUM_ = 'Sum'
                                if 'Minimum' in metric_desc:
                                    MIN_ = 'Minimum'
                                if 'Maximum' in metric_desc:
                                    MAX_ = 'Maximum'
                                if 'Average' in metricdesc:
                                    AVG_ = 'Average'
                            elif var2.startswith('Stat')or idx>0:
                                metric_statistics = var2
                                if 'Sum' in metric_statistics:
                                    SUM_ = 'Sum'
                                if 'Minimum' in metric_statistics:
                                    MIN_ = 'Minimum'
                                if 'Maximum' in metric_statistics:
                                    MAX_ = 'Maximum'
                                if 'Average' in metric_statistics:
                                    AVG_ = 'Average'

                    self.mapping.append('elb.' + op.replace('aws.elb.','')+' '
                        'aws_elb_' + op.replace('aws.elb.','' ))
                    self.add_to_list(self.aws_list, op, metric_desc, )
                    #print(metric_statistics)
                    if MIN_:
                        self.add_to_list(self.aws_list, op + ".minimum", metric_desc)
                    if MAX_:
                        self.add_to_list(self.aws_list, op + ".maximum", metric_desc)
                    if AVG_:
                        self.add_to_list(self.aws_list, op + ".average", metric_desc )
                    if SUM_:
                        self.add_to_list(self.aws_list, op + ".sum", metric_desc)

                SUM_ = ''
                MIN_ = ''
                MAX_ = ''
                AVG_ = ''

            else:
                if originalmetricname in CODE_MAP.keys():
                    metric_name = 'aws.elb.' + CODE_MAP[originalmetricname]
                if metric_name == 'aws.elb.h_t_t_p_code__e_l_b_4_x_x':
                    metric_name = str('aws.elb.' + 'http_code_elb_4xx')
                if metric_name == 'aws.elb.h_t_t_p_code__e_l_b_5_x_x':
                    metric_name = str('aws.elb.' + 'http_code_elb_5xx')
                colone = cols[1]
                sections = colone.findChildren('p')
                if sections and len(sections) > 0:
                    idx = 0
                    for sect in sections:
                        descriptions = sect.findAll(text=True)
                        var1 = ' '.join(descriptions)
                        var1 = var1.replace('\n', '')
                        var2 = ' '.join(var1.split())
                        if var2 and idx == 0:
                            metric_desc = var2
                            metricdesc = var2
                            if 'Sum' in metricdesc:
                                SUM_ = 'Sum'
                            if 'Minimum' in metricdesc:
                                MIN_ = 'Minimum'
                            if 'Maximum' in metricdesc:
                                MAX_ = 'Maximum'
                            if 'Average' in metricdesc:
                                AVG_ = 'Average'
                        elif var2.startswith('Statistics:')or idx>0:
                            metric_ststone = var2

                            if 'Sum' in metric_ststone:
                                SUM_ = 'Sum'
                            if 'Minimum' in metric_ststone:
                                MIN_ = 'Minimum'
                            if 'Maximum' in metric_ststone:
                                MAX_ = 'Maximum'
                            if 'Average' in metric_ststone:
                                AVG_ = 'Average'
                        elif var2.startswith('Example'):
                            metric_example = var2
                        idx = idx + 1


                    self.mapping.append('elb.' + metric_name.replace('aws.elb.','')+' '+
                                             'aws_elb_' + metric_name.replace('aws.elb.',''))

                    self.add_to_list(self.aws_list, metric_name, metric_desc, )
                    if MIN_:
                        self.add_to_list(self.aws_list, metric_name + ".minimum", metric_desc)
                    if MAX_:
                        self.add_to_list(self.aws_list, metric_name + ".maximum", metric_desc)
                    if AVG_:
                        self.add_to_list(self.aws_list, metric_name + ".average", metric_desc )
                    if SUM_:
                        self.add_to_list(self.aws_list, metric_name + ".sum", metric_desc)


                    #self.mapping.append(['elb.' + self.convertToSnakeCase(ogmetricfive_name),
                                        #'aws_elb_' + self.convertToSnakeCase(ogmetricfive_name)])




        matchtwo = soup.find('table', id="w282aac21b7c13c11")
        matchtworows = matchtwo.findAll('tr')
        for j in matchtworows[1:]:
            cols = j.findAll('td')
            col = cols[0]
            colone = cols[1]
            coltext = col.text.strip()
            self.aws_list2.append([coltext,"None"])
            met_name = 'aws.elb.' + self.snake_case(col.text.strip())
            met_un= 'guage'
            if coltext in CODE_MAP.keys():
                met_name = CODE_MAP[coltext]

            met_desc = (colone.text.strip())
            # print(met_desc)
            self.mapping.append('elb.' + met_name.replace('aws.elb.', '')+' '+
                                 'aws_elb_' + met_name.replace('aws.elb.', ''))

            self.add_to_list(self.aws_list, met_name, met_desc)

            #self.mapping.append(['elb.' + self.convertToSnakeCase(ogmetricfive_name),
                                 #'aws_elb_' + self.convertToSnakeCase(ogmetricfive_name)])

        mathcthree = soup.find('table', id="w282aac21b7c15b5")
        matchthreerows = mathcthree.findAll('tr')
        for l in matchthreerows[1:]:
            colson = l.findAll('td')
            coly = colson[0]
            colones = colson[1]
            met_nameone = self.snake_case(coly.text.strip())
            if coly in CODE_MAP.keys():
                met_nameone = CODE_MAP[coly]
            self.aws_dict['keys'].append(
                {'name': met_nameone, 'alias': 'dimension_' + coly.text.strip()})
            # print(met_name)
            met_descone = (colone.text.strip())
            # print(met_desc)
        #print(self.mapping)
        #for i in self.mapping:
        self.add_to_list(self.aws_list, httpcode2, metric_desc, )
        self.add_to_list(self.aws_list, httpcode2 + ".minimum", metric_desc )
        self.add_to_list(self.aws_list, httpcode2 + ".maximum", metric_desc)
        self.add_to_list(self.aws_list, httpcode2 + ".average", metric_desc )
        self.mapping.append('elb.' + httpcode2.replace('aws.elb.', '') + ' '
                                                                         'aws_elb_' + httpcode2.replace('aws.elb.', ''))



    def generate_csv(self):
        os.chdir('CSV_FOLDER')
        with open(CSV_FILE, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(mn)
            writer.writerows(self.aws_list)
        os.chdir('..')

    def generate_csv2(self):
        os.chdir('CSV_METRIC_NAMES')
        with open(CSV2, 'w', newline='') as f:
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
    def add_to_list(aws_list, metric_name, description):
        #print(metric_name, "||", description, )
        aws_list.append([metric_name, "guage", "", "", "", description, "", "elb", ""])

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
    extractor = ELBExtractor(
        'https://docs.aws.amazon.com/elasticloadbalancing/latest/classic/elb-cloudwatch-metrics.html')
    extractor.load_page()
    extractor.process_content()
    extractor.generate_yaml()
    extractor.generate_csv()
    extractor.generate_csv2()
    extractor.generateMapping()