import os
from DynamoDB import DynamoDB
from EC2AutoScaling import EC2AutoExtractor
from EC2Spot import EC2SpotExtractor
from Kinesis import Kinesis
from LambaScraping import LambdaExtractor

CSV_FOLDER = 'CSV_FOLDER'
YAML_FOLDER = 'YAML_FOLDER'

class AWSMetricsRunner:
    def __init__(self,csvName = '', yamlName = ''):
        self.csv_folder = csvName
        self.yaml_folder = yamlName

    def generate_folders(self):
        path = './'
        os.chdir(path)
        if not os.path.exists(CSV_FOLDER):
            NewFolder = CSV_FOLDER
            os.mkdir(NewFolder)
        if not os.path.exists(YAML_FOLDER):
            NewFolder2 = YAML_FOLDER
            os.mkdir(NewFolder2)


if __name__  == '__main__':
    creator = AWSMetricsRunner("CSV_FOLDER","YAML_FOLDER")
    creator.generate_folders()
    DynamoDBrunner = DynamoDB('https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/metrics-dimensions.html')
    print("Processing Metric: DynamoDB")
    DynamoDBrunner.load_page()
    DynamoDBrunner.process_content()
    DynamoDBrunner.generate_yaml()
    DynamoDBrunner.generate_csv()
    print("Finished Metric: DynamoDB")
    EC2AutoRunner = EC2AutoExtractor('https://docs.aws.amazon.com/autoscaling/ec2/userguide/as-instance-monitoring.html')
    EC2AutoRunner.load_page()
    EC2AutoRunner.process_content()
    print("Processing Metric: EC2AutoScaling")
    EC2AutoRunner.generate_yaml()
    EC2AutoRunner.generate_csv()
    print("Finished Metric: EC2AutoScaling")
    EC2SpotRunner = EC2SpotExtractor('https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/spot-fleet-cloudwatch-metrics.html')
    EC2SpotRunner.load_page()
    EC2SpotRunner.process_content()
    print("Processing Metric: EC2Spot")
    EC2SpotRunner.generate_yaml()
    EC2SpotRunner.generate_csv()
    print("Finished Metric: EC2Spot")
    KinesisRunner = Kinesis('https://docs.aws.amazon.com/streams/latest/dev/monitoring-with-cloudwatch.html')
    KinesisRunner.load_page()
    KinesisRunner.process_content()
    print("Processing Metric: Kinesis")
    KinesisRunner.generate_yaml()
    KinesisRunner.generate_csv()
    print("Finished Metric: Kinesis")
    LambaRunner = LambdaExtractor('https://docs.aws.amazon.com/lambda/latest/dg/monitoring-metrics.html')
    LambaRunner.load_page()
    LambaRunner.process_content()
    print("Processing Metric: Lambda")
    LambaRunner.generate_yaml()
    LambaRunner.generate_csv()
    print("Finished Metric: Lambda")
    print("Task Completed Successfully")


