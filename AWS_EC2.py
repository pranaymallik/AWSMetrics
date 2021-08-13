import requests
from bs4 import BeautifulSoup
import re
import csv
import yaml
import os

res = requests.get('https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/viewing_metrics_with_cloudwatch.html')
UNITS_ = 'Units:'
CSV_FILE = 'AWS.EC2.csv'
CSV2 = 'AWS.stats.EC2.csv'
YAML_File = 'AWS.EC2.yaml'
MAPPING_FILE = 'AWS.EC2.MAPPING.TEXT'
METRIC_HEADERS = ["metric_name", "metric_type", "interval", "unit_name", "per_unit_name", "description", "orientation",
                  "integration", "short_name", ]
lineadder = ['Minimum', 'Maximum', 'Average']
Statistics = 'Statistics: '
mn = ['Metric Name',"Metric Stats"]
CODE_MAP = {
    'CPUUtilization': 'aws.ec2.cpu_utilization',
    'EBSIOBalance%': 'aws.ec2.ebs_io_balance%',
    'CPUCreditUsage': 'aws.ec2.cpu_credit_usage',
    'CPUCreditBalance': 'aws.ec2.cpu_credit_balance',
    'CPUSurplusCreditBalance': 'aws.ec2.cpu_surplus_credit_balance',
    'CPUSurplusCreditsCharged': 'aws.ec2.cpu_surplus_credits_charged',
    'EBSReadOps': 'aws.ec2.ebs_read_ops',
    'EBSWriteOps': 'aws.ec2.ebs_write_ops',
    'EBSWriteBytes': 'aws.ec2.ebs_write_bytes',
    'EBSReadBytes': 'aws.ec2.ebs_read_bytes',
    'EBSIobalance%': 'aws.ec2.ebs_iobalance%',
    'EBSBytesbalance%': 'aws.ec2.ebs_byte_balance%',
    '" Minimum, Maximum, Average"':'Minimum, Maximum, Average'

}



class AWSEc2Extractor:
    def __init__(self, url):
        self.url = url
        self.content = ""
        self.aws_dict = {}
        self.aws_list = []
        self.aws_list2 = []
        self.mapping = []
        self.yaml = []

    def load_page(self):
        page = requests.get(self.url)
        if page.status_code == 200:
            self.content = page.content

    def get_content(self):
        return self.content

    def process_content(self):
        soup = BeautifulSoup(res.text, 'html.parser')
        # match = soup.find('div', {'id': 'main-content'})
        # tbodyone = match.find('tbody')
        self.aws_dict = {'type': 'ec2', 'keys': []}
        matchone = soup.findAll('table')
        rows = matchone[0].findAll('tr')
        tableonerows = matchone[1].findAll('tr')
        tabletworows = matchone[2].findAll('tr')
        tablethreerows = matchone[3].findAll('tr')
        tablefourrows = matchone[4].findAll('tr')
        table5 = matchone[5].findAll('tr')
        table6rows = matchone[6].findAll('tr')
        met_s = ''

        for i in rows[1:]:
            cols = i.findAll('td')
            if cols and len(cols) > 0:
                col = cols[0]
                ogmetric_name = col.text.strip()
                metric_name = 'aws.ec2.' + self.convertToSnakeCase(ogmetric_name)
                metr_stats = ''
                met_desc = ''
                met_unit = ''
                met_stats = ''
                if metric_name.startswith('cpu'):  # redo this
                    metric_name = metric_name.replace('cpu', 'cpu_')  # redo this
                if ogmetric_name in CODE_MAP.keys():
                    metric_name= CODE_MAP[ogmetric_name]
                coloftone = cols[1]
                secon = coloftone.findChildren('p')
                if secon and len(secon) > 0:
                    idx = 0
                    for u in secon:
                        descriptions = u.findAll(text=True)
                        var1 = ' '.join(descriptions)
                        var1 = var1.replace('\n', '')
                        var2 = ' '.join(var1.split())
                        # print(var2)
                        if var2 and idx == 0 and not var2.startswith('Units') and not var2.startswith('Statistics'):
                            met_desc = var2
                        elif var2.startswith('Units'):
                            met_unit = var2
                            if 'Count' not in met_unit:
                                met_unit = 'guage'
                            else:
                                met_unit = 'count'
                        elif var2.startswith('Statistics'):
                            met_stats = var2
                            met_stats = var2.replace('Statistics:','').replace('"','')
                            metr_stats = met_stats
                            if met_stats in CODE_MAP.keys():
                                met_stats = CODE_MAP[met_stats]
                        if met_stats =='':
                            met_stats = 'None'

                        idx + idx + 1
                    self.add_to_list(self.aws_list, metric_name, met_unit, met_desc)
                    if ' Minimum, Maximum, Average'in met_stats:
                        met_stats = 'Minimum, Maximum, Average'
                    print(met_stats)
                    self.aws_list2.append([ogmetric_name,met_stats])
                    self.mapping.append('ec2.'+self.convertToSnakeCase(metric_name).replace('aws.ec2.','')+' '+'aws_ec2_'+self.convertToSnakeCase(metric_name).replace('aws.ec2.',''))

                    if metr_stats:
                        self.add_to_list(self.aws_list, metric_name + ".minimum", met_unit, met_desc + ".minimum")
                        self.add_to_list(self.aws_list, metric_name + ".maximum", met_unit, met_desc + ".maximum")
                        self.add_to_list(self.aws_list, metric_name + ".average", met_unit, met_desc + ".average")


                    # print(metric_name+" : ",met_desc," : "+met_unit)

                # print(metric_name)
        for e in tableonerows[1:]:
            colsthree = e.findAll('td')
            if colsthree and len(colsthree) > 0:
                met_unitone = ''
                met_descone = ''
                colthree = colsthree[0]
                ogmetricthree_name = colthree.text.strip()
                metric_namethree = 'aws.ec2.' + self.convertToSnakeCase(ogmetricthree_name) # redo this
                    # print(metric_namethree)
                # print(metric_namethree)
                if ogmetricthree_name in CODE_MAP.keys():
                    metric_namethree = CODE_MAP[ogmetricthree_name]
                coloftthree = colsthree[1]
                seconthree = coloftthree.findChildren('p')
                if seconthree and len(seconthree) > 0:
                    idx = 0
                    for z in seconthree:
                        descriptionsthree = z.findAll(text=True)
                        varz = ' '.join(descriptionsthree)
                        varz = varz.replace('\n', '')
                        vary = ' '.join(varz.split())
                        # print(var2)
                        if vary and idx == 0 and not vary.startswith('Units') and not vary.startswith('Statistics'):
                            met_descone = vary
                        elif vary.startswith('Units'):
                            met_unitone = vary

                            if 'Count' not in met_unitone:
                                met_unitone = 'gauge'
                            else:
                                met_unitone = 'count'
                        elif idx == 1 and 'Sum' in vary:
                            met_s = 'Fill'
                        idx = idx + 1
                    self.add_to_list(self.aws_list, metric_namethree, met_unitone, met_descone)
                    self.aws_list2.append([ogmetricthree_name,"None"])
                    if met_s:
                        self.add_to_list(self.aws_list, metric_namethree+'.sum', met_unitone, met_descone)
                        met_s = ''

                    self.mapping.append('ec2.' + self.convertToSnakeCase(metric_namethree).replace('aws.ec2.','')+ ' '+
                                        'aws_ec2_' + self.convertToSnakeCase(metric_namethree).replace('aws.ec2.',''))

        for j in tabletworows[1:]:
            colstwo = j.findAll('td')
            if colstwo and len(colstwo) > 0:
                met_desctwo = ''
                met_units2 = ''
                met_stat = ''
                coltwo = colstwo[0]
                ogmetrictwo_name = coltwo.text.strip()
                metric_nametwo = 'aws.ec2.' + self.convertToSnakeCase(ogmetrictwo_name)
                #print(CODE_MAP.keys())
                #print(ogmetrictwo_name)
                #break
                if ogmetrictwo_name in CODE_MAP.keys():

                    metric_nametwo = CODE_MAP[ogmetrictwo_name]
                # print(metric_nametwo)
                colofttwo = colstwo[1]
                secontwo = colofttwo.findChildren('p')
                idx = 0
                if secontwo and len(secontwo) > 0:

                    for o in secontwo:
                        descriptionsthwo = o.findAll(text=True)
                        varh = ' '.join(descriptionsthwo)
                        varh = varh.replace('\n', '')
                        vara = ' '.join(varh.split())
                        # print(var2)
                        if vara and idx == 0 and not vara.startswith('Units') and not vara.startswith('Statistics'):
                            met_desctwo = vara
                        elif vara.startswith('Units'):
                            met_units2 = vara
                            if 'Count' not in met_units2:
                                met_units2 = 'gauge'
                            else:
                                met_units2 = 'count'
                            if met_units2 == "":
                                met_units2 = 'gauge'
                        elif 'statistic' in vara:
                            met_stat = vara
                        idx = idx + 1

                    self.add_to_list(self.aws_list, metric_nametwo,'gauge', met_desctwo)
                    if met_stat:
                        self.add_to_list(self.aws_list, metric_nametwo+'.sum','gauge', met_desctwo)


                    self.aws_list2.append([ogmetrictwo_name, "None"])
                    self.mapping.append('ec2.' + self.convertToSnakeCase(metric_nametwo).replace('aws.ec2.','')+' '+
                                        'aws_ec2_' + self.convertToSnakeCase(metric_nametwo).replace('aws.ec2.',''))



        for w in tablethreerows[1:]:
            colsfour = w.findAll('td')
            if colsfour and len(colsfour) > 0:
                met_descone4 = ''
                met_unitone4 = ''
                colfour = colsfour[0]
                ogmetricfour_name = colfour.text.strip()
                metric_namefour = 'aws.ec2.' + self.convertToSnakeCase(ogmetricfour_name)
                if metric_namefour.startswith('cpu'):  # redo this
                    metric_namefour = metric_namefour.replace('cpu', 'cpu_')  # redo this
                # print(metric_namefour)
                if ogmetricfour_name in CODE_MAP.keys():
                    metric_namefour = CODE_MAP[ogmetricfour_name]
                colofour = colsfour[1]
                secon4 = colofour.findChildren('p')
                if secon4 and len(secon4) > 0:
                    idx = 0
                    for b in secon4:
                        descriptions4 = b.findAll(text=True)
                        vare = ' '.join(descriptions4)
                        vare = vare.replace('\n', '')
                        vart = ' '.join(vare.split())
                        # print(var2)
                        if vart and idx == 0 and not vart.startswith('Units'):
                            met_descone4 = vart
                        elif vart.startswith('Units'):
                            met_unitone4 = vart
                            if 'Count' not in met_unitone4:
                                met_unitone4 = 'guage'
                            else:
                                met_unitone4 = 'count'
                        idx + idx + 1
                    self.aws_list2.append([ogmetricfour_name, "None"])
                    self.add_to_list(self.aws_list, metric_namefour, 'gauge', met_descone4)
                    self.mapping.append('ec2.' + self.convertToSnakeCase(metric_namefour).replace('aws.ec2.','')+' '+
                                        'aws_ec2_' + self.convertToSnakeCase(metric_namefour).replace('aws.ec2.',''))

        for s in tablefourrows[1:]:
            colsfive = s.findAll('td')
            if colsfive and len(colsfive) > 0:
                met_desconefive = ''
                met_unitonefive = ''
                met_statsone = ''
                colfive = colsfive[0]
                ogmetricfive_name = colfive.text.strip()
                metric_namefive = 'aws.ec2.' + self.convertToSnakeCase(ogmetricfive_name)
                if metric_namefive.startswith('cpu'):  # redo this
                    metric_namefive = metric_namefive.replace('cpu', 'cpu_')  # redo this
                if ogmetricfive_name in CODE_MAP.keys():
                    metric_namefive = CODE_MAP[ogmetricfive_name]
                # print(metric_namefive)
                coloftfive = colsfive[1]
                seconfive = coloftfive.findChildren('p')
                if seconfive and len(seconfive) > 0:
                    idx = 0
                    for q in seconfive:
                        descriptionsfive = q.findAll(text=True)
                        varq = ' '.join(descriptionsfive)
                        varq = varq.replace('\n', '')
                        varr = ' '.join(varq.split())
                        # print(var2)
                        if varr and idx == 0 and not varr.startswith('Units'):
                            met_desconefive = varr
                        elif varr.startswith('Units'):
                            met_unitonefive = varr
                            if 'Count' not in met_unitonefive:
                                met_unitonefive = 'guage'
                            else:
                                met_unitonefive = 'count'
                        elif 'statistics' in varr:
                            met_statsone = varr
                        idx + idx + 1
                    self.add_to_list(self.aws_list, metric_namefive, 'gauge', met_desconefive)
                    self.aws_list2.append([ogmetricfive_name, "None"])
                    self.mapping.append('ec2.' + self.convertToSnakeCase(metric_namefive).replace('aws.ec2.','')+' '+
                                        'aws_ec2_' + self.convertToSnakeCase(metric_namefive).replace('aws.ec2.',''))

                    if met_statsone:
                        self.add_to_list(self.aws_list, metric_namefive + ".minimum", met_unitonefive,
                                         met_desconefive + ".minimum")
                        self.add_to_list(self.aws_list, metric_namefive + ".maximum", met_unitonefive,
                                         met_desconefive + ".maximum")
                        self.add_to_list(self.aws_list, metric_namefive + ".average", met_unitonefive,
                                         met_desconefive + ".average")
        for d in table5[1:]:
            colssix = d.findAll('td')
            if colssix and len(colssix) > 0:
                met_desconesix = ''
                met_statsonesix = ''
                met_unitonesix = ''
            colsix = colssix[0]
            ogmetricsix_name = colsix.text.strip()
            metric_namesix = 'aws.ec2.' + self.convertToSnakeCase(ogmetricsix_name)
            coloftsix = colssix[1]
            seconsix = coloftsix.findChildren('p')
            if seconsix and len(seconsix) > 0:
                idx = 0
                for p in seconsix:
                    descriptionssix = p.findAll(text=True)
                    varp = ' '.join(descriptionssix)
                    varp = varp.replace('\n', '')
                    var = ' '.join(varp.split())
                    # print(var2)
                    if var and idx == 0 and not var.startswith('Units'):
                        met_desconesix = varr
                    elif var.startswith('Units'):
                        met_unitonesix = varr
                    elif var.startwith('Statistics:'):
                        met_statsonesix = varr
                    idx + idx + 1
                self.add_to_list(self.aws_list, metric_namesix, 'gauge', met_desconesix)
                self.aws_list2.append([ogmetricsix_name, "None"])
                self.mapping.append('ec2.' + self.convertToSnakeCase(ogmetricsix_name)+' '+
                                    'aws_ec2_' + self.convertToSnakeCase(ogmetricsix_name))

                #self.aws_list2.append([met_statsonesix])


                self.add_to_list(self.aws_list, metric_namesix + ".maximum", met_unitonesix,
                                     met_desconefive + ".maximum")

        for t in table6rows[1:]:
            colp = t.findAll('td')
            co = colp[0]
            orig = co.text.strip()
            metseven = self.snake_case(orig)
            self.aws_dict['keys'].append(
                {'name': metseven, 'alias': 'dimension_' + co.text.strip()})
            #self.yaml = ["- keys:  - name: ".replace+ metseven,"    alias: "+ co.text.strip(),"type: ec2"]
        print(self.mapping)

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
      #  print(self.aws_dict)
        with open(YAML_File, 'w') as outfile:
            yaml.dump([self.aws_dict], outfile, default_flow_style=False)
        os.chdir('..')
    @staticmethod
    def add_to_list(aws_list, metric_name, met_units, description):

        #print(metric_name, "||", met_units, "||", description, )
        aws_list.append([metric_name, met_units, "", "", "", description, "", "ec2", ""])

    @staticmethod
    def add_to_list2(aws_list, metric_name,met_stats):
        # print(metric_name, '||', metric_type, "||", description, )
        aws_list.append([metric_name,met_stats])

    @staticmethod
    def snake_case(input_string):

        if input_string:
            return re.sub(r'(?<!^)(?=[A-Z])', '_', input_string).lower()
        else:
            return input_string

    @staticmethod
    def convertToSnakeCase(name):
        s1 = re.sub('(.^_)([A-Z][a-z]+)', r'\1_\2', name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
    def generateMapping(self):
        os.chdir('MAPPING_FOLDER')
        with open(MAPPING_FILE, 'w') as filehandle:
            for listitem in self.mapping:
                filehandle.write(str(listitem)+'\n' )
        os.chdir('..')

if __name__ == "__main__":
    extractor = AWSEc2Extractor('https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/viewing_metrics_with_cloudwatch.html')
    extractor.load_page()
    extractor.process_content()
    extractor.generate_yaml()
    extractor.generate_csv()
    extractor.generate_csv2()
    extractor.generateMapping()
