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
mn = ["metric_name", "metric_type", "interval", "unit_name", "per_unit_name", "description", "orientation",
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
        SUM_ = ''
        MIN_ = ''
        MAX_ = ''
        AVG_ = ''
        p50_ = ''
        p90_ = ''
        p95_ = ''
        p99_ = ''
        p99_99_ = ''
        soup = BeautifulSoup(self.content, 'html.parser')
        main_content = soup.findAll('table')[1]
        main_cont = soup.findAll('table')[0]
        rows = main_content.findAll('tr')[1:]
        rowe = main_cont.findAll('tr')[1:]
        metric_name = ''
        self.aws_dict = {'type': 's3', 'keys': []}
        self.aws_list = []
        for temp in rowe:
            cols = temp.findAll('td')
            if cols and len(cols) > 0:
                col = cols[0]
                original_metric_name = col.text.strip()

                metric_name_snake_case = self.snake_case(original_metric_name)
                metric_name = 'aws.s3.' + metric_name_snake_case
                # print(metric_name)

            if cols and len(cols) > 1:
                col = cols[1]
                sections = col.findChildren('p', text=True)
                if sections and len(sections) > 0:
                    idx = 0
                    metric_description = ''
                    metric_stats = ''
                    metric_units = ''
                    for section in sections:
                        if section:
                            section_string = section.string.strip()
                            if section_string and idx == 0:
                                section_string = " ".join(section_string.split())
                                section_string = section_string.replace('\n', '')
                                metric_description = section_string
                            elif section_string.startswith(VALID_STATISTICS_):
                                metric_stats = section_string.replace(VALID_STATISTICS_, '').strip()
                                metric_stats = " ".join(metric_stats.split())
                                metric_stats = metric_stats.replace('\n', '')
                                metric_stats = re.sub(r"\([^()]*\)", "", metric_stats).replace(' ', '')
                                if 'Sum' in metric_stats:
                                    SUM_ = 'Sum'
                                if 'Minimum' in metric_stats:
                                    MIN_ = 'Minimum'
                                if 'Maximum' in metric_stats:
                                    MAX_ = 'Maximum'
                                if 'Average' in metric_stats:
                                    AVG_ = 'Average'
                                if 'p0.0' in metric_stats:
                                    p90_ = 'p90'


                            elif section_string.startswith(UNITS_):
                                metric_units = section_string.replace(UNITS_, '').strip()
                            if metric_stats == '':
                                metric_stats = 'None'
                        idx = idx + 1

                        if metric_units != 'count':
                            metric_units = 'gauge'
                self.add_to_list(self.aws_list, metric_name, metric_units, metric_description)
                if MIN_:
                    self.add_to_list(self.aws_list, metric_name + ".minimum",metric_units, metric_description)
                if MAX_:
                    self.add_to_list(self.aws_list, metric_name + ".maximum",metric_units, metric_description)
                if AVG_:
                    self.add_to_list(self.aws_list, metric_name + ".average",metric_units, metric_description)
                if SUM_:
                    self.add_to_list(self.aws_list, metric_name + ".sum", metric_units,metric_description)
                if p90_:
                    for suffix in LATENCY_VALUES:
                        self.add_to_list(self.aws_list, metric_name + '.' + suffix, metric_units,
                                                     metric_stats,
                                                     self.update_description(metric_description, suffix))


                self.add_to_list2(self.aws_list2, original_metric_name, metric_stats)
                self.mapping.append('s3.' + metric_name.replace('aws.s3.', '') + ' ' +
                             'aws_s3_' + metric_name.replace('aws.s3.', ''))
        SUM_ = ''
        MIN_ = ''
        MAX_ = ''
        AVG_ = ''
        p50_ = ''
        p90_ = ''
        for row in rows:
            cols = row.findAll('td')
            if cols and len(cols) > 0:
                col = cols[0]
                original_metric_name = col.text.strip()

                metric_name_snake_case = self.snake_case(original_metric_name)
                metric_name = 'aws.s3.' + metric_name_snake_case
                # print(metric_name)

            if cols and len(cols) > 1:
                col = cols[1]
                sections = col.findChildren('p', text=True)
                if sections and len(sections) > 0:
                    idx = 0
                    metric_description = ''
                    metric_stats = ''
                    metric_units = ''
                    for section in sections:
                        if section:
                            section_string = section.string.strip()
                            if section_string and idx == 0:
                                section_string = " ".join(section_string.split())
                                section_string = section_string.replace('\n', '')
                                metric_description = section_string
                            elif section_string.startswith(VALID_STATISTICS_):
                                metric_stats = section_string.replace(VALID_STATISTICS_, '').strip()
                                metric_stats = " ".join(metric_stats.split())
                                metric_stats = metric_stats.replace('\n', '')
                                metric_stats = re.sub(r"\([^()]*\)", "", metric_stats).replace(' ', '')
                                if 'Sum' in metric_stats:
                                    SUM_ = 'Sum'
                                if 'Minimum' in metric_stats:
                                    MIN_ = 'Minimum'
                                if 'Maximum' in metric_stats:
                                    MAX_ = 'Maximum'
                                if 'Average' in metric_stats:
                                    AVG_ = 'Average'
                                if 'p0.0' in metric_stats:
                                    p90_ = 'p90'


                            elif section_string.startswith(UNITS_):
                                metric_units = section_string.replace(UNITS_, '').strip()
                            if metric_stats == '':
                                metric_stats = 'None'
                        idx = idx + 1


                    if metric_units != 'count':
                        metric_units = 'gauge'
                    self.add_to_list(self.aws_list, metric_name, metric_units, metric_description)
                    if MIN_:
                        self.add_to_list(self.aws_list, metric_name + ".minimum", metric_units, metric_description)
                    if MAX_:
                        self.add_to_list(self.aws_list, metric_name + ".maximum", metric_units, metric_description)
                    if AVG_:
                        self.add_to_list(self.aws_list, metric_name + ".average", metric_units, metric_description)
                    if SUM_:
                        self.add_to_list(self.aws_list, metric_name + ".sum", metric_units, metric_description)
                    if p90_:
                        for suffix in LATENCY_VALUES:
                            self.add_to_list(self.aws_list, metric_name + '.' + suffix, metric_units,

                                             self.update_description(metric_description, suffix))

                    self.add_to_list2(self.aws_list2, original_metric_name, metric_stats)
                    self.mapping.append('s3.' + metric_name.replace('aws.s3.', '') + ' ' +
                                        'aws_s3_' + metric_name.replace('aws.s3.', ''))
        SUM_ = ''
        MIN_ = ''
        MAX_ = ''
        AVG_ = ''
        p50_ = ''
        p90_ = ''
        matchone = soup.findAll('table')
        rowsone = matchone[2].findAll('tr')
        rowsfour = matchone[3].findAll('tr')
        for var in rowsone[1:]:
            colone = var.findAll('td')
            coly = colone[0]
            ogmet = coly.text.strip()
            metric_name = 'aws.s3.' + self.snake_case(ogmet)
            # print(metric_name)
            colonetwo = colone[1]
            sec = colonetwo.findChildren('p')
            if sec and len(sec) > 0:
                met_desc = ''
                metric_units = ''
                metric_stats = ''
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
                            metric_stats = var2.replace('Valid statistics: ', '')
                            if '(' in metric_stats:
                                metric_stats = re.sub(r"\([^()]*\)", "", metric_stats).replace(' ', '')
                                if 'Sum' in metric_stats:
                                    SUM_ = 'Sum'
                                if 'Minimum' in metric_stats:
                                    MIN_ = 'Minimum'
                                if 'Maximum' in metric_stats:
                                    MAX_ = 'Maximum'
                                if 'Average' in metric_stats:
                                    AVG_ = 'Average'
                                if 'p0.0' in metric_stats:
                                    p90_ = 'p90'


                            # print(metric_stats)
                        elif var2.startswith(UNITS_):
                            metric_units = var2
                            metric_units = metric_units.replace(UNITS_, '').strip()
                            if metric_units.lower() != 'counts':

                                metric_units = 'gauge'
                            else:
                                metric_units = 'count'

                        idx = idx + 1

                    self.add_to_list2(self.aws_list2, ogmet, metric_stats)
                    self.add_to_list(self.aws_list, metric_name, metric_units, met_desc)
                if MIN_:
                    self.add_to_list(self.aws_list, metric_name + ".minimum",metric_units, metric_description)
                if MAX_:
                    self.add_to_list(self.aws_list, metric_name + ".maximum",metric_units, metric_description)
                if AVG_:
                    self.add_to_list(self.aws_list, metric_name + ".average",metric_units, metric_description)
                if SUM_:
                    self.add_to_list(self.aws_list, metric_name + ".sum", metric_units,metric_description)
                if p90_:
                    for suffix in LATENCY_VALUES:
                        self.add_to_list(self.aws_list, metric_name + '.' + suffix, metric_units,

                                                     self.update_description(metric_description, suffix))



                    self.mapping.append('s3.' + metric_name.replace('aws.s3.', '') + ' '
                                                                                  'aws_s3_' + metric_name.replace('aws.s3.',
                                                                                                               ''))
        SUM_ = ''
        MIN_ = ''
        MAX_ = ''
        AVG_ = ''
        p50_ = ''
        p90_ = ''
        # print(self.mapping)
        for var in rowsfour:
            cols = var.findAll('td')
            if cols and len(cols) > 0:
                col = cols[0]
                original_metric_name = col.text.strip()

                metric_name_snake_case = self.snake_case(original_metric_name)
                metric_name = 'aws.s3.' + metric_name_snake_case
                # print(metric_name)

            if cols and len(cols) > 1:
                col = cols[1]
                sections = col.findChildren('p', text=True)
                if sections and len(sections) > 0:
                    idx = 0
                    metric_description = ''
                    metric_stats = ''
                    metric_units = ''
                    for section in sections:
                        if section:
                            section_string = section.string.strip()
                            if section_string and idx == 0:
                                section_string = " ".join(section_string.split())
                                section_string = section_string.replace('\n', '')
                                metric_description = section_string
                            elif section_string.startswith(VALID_STATISTICS_):
                                metric_stats = section_string.replace(VALID_STATISTICS_, '').strip()
                                metric_stats = " ".join(metric_stats.split())
                                metric_stats = metric_stats.replace('\n', '')
                                metric_stats = re.sub(r"\([^()]*\)", "", metric_stats).replace(' ', '')
                            elif section_string.startswith(UNITS_):
                                metric_units = section_string.replace(UNITS_, '').strip()
                            if metric_stats == '':
                                metric_stats = 'None'
                            if 'Sum' in metric_stats:
                                SUM_ = 'Sum'
                            if 'Minimum' in metric_stats:
                                MIN_ = 'Minimum'
                            if 'Maximum' in metric_stats:
                                MAX_ = 'Maximum'
                            if 'Average' in metric_stats:
                                AVG_ = 'Average'
                            if 'p0.0' in metric_stats:
                                p90_ = 'p90'

                        idx = idx + 1


                    metric_units = metric_units.lower()
                    if metric_units != 'count':
                        metric_units = 'gauge'
                    self.add_to_list(self.aws_list, metric_name, metric_units,  metric_description)

                    if MIN_:
                        self.add_to_list(self.aws_list, metric_name + ".minimum", metric_units, metric_description)
                    if MAX_:
                        self.add_to_list(self.aws_list, metric_name + ".maximum", metric_units, metric_description)
                    if AVG_:
                        self.add_to_list(self.aws_list, metric_name + ".average", metric_units, metric_description)
                    if SUM_:
                        self.add_to_list(self.aws_list, metric_name + ".sum", metric_units, metric_description)
                    if p90_:
                        for suffix in LATENCY_VALUES:
                            self.add_to_list(self.aws_list, metric_name + '.' + suffix, metric_units,

                                             self.update_description(metric_description, suffix))

                    self.add_to_list2(self.aws_list2, original_metric_name, metric_stats)
                    self.mapping.append('s3.' + metric_name.replace('aws.s3.', '') + ' ' +
                                        'aws_s3_' + metric_name.replace('aws.s3.', ''))
        for t in rowsfour[1:]:
            colo = t.findAll('td')[0]
            yamlmetname = self.snake_case(colo.text.strip())
            ogmetyaml = colo.text.strip()
            self.aws_dict['keys'].append(
                {'name': yamlmetname, 'alias': 'dimension_' + ogmetyaml})


    # print(self.mapping)

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
    def add_to_list(aws_list, metric_name, units, description ):
        print(metric_name, '||', units, '||', description, )
        aws_list.append([metric_name, units, "", "", "", description, "", "s3", "", ])


    @staticmethod
    def add_to_list2(aws_list, metric_name, metric_stats):
        # print(metric_name, '||', metric_type, "||", description, )
        aws_list.append([metric_name, metric_stats])


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
                filehandle.write(  str(listitem)+'\n')
        os.chdir('..')


if __name__ == "__main__":
    extractor = AWSS3Extractor('https://docs.aws.amazon.com/AmazonS3/latest/userguide/metrics-dimensions.html')
    extractor.load_page()
    extractor.process_content()
    extractor.generate_yaml()
    extractor.generate_csv()
    extractor.generate_csv2()
    extractor.generateMapping()
