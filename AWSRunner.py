import os
from AWS_DynamoDB import DynamoDB
from AWS_EC2_Auto_Scaling import EC2AutoExtractor
from AWS_EC2Spot import EC2SpotExtractor
from AWS_Kinesis import Kinesis
from AWS_Lambda import LambdaExtractor
from AWS_S3 import AWSS3Extractor
from AWS_RDS import AWSRDSExtractor
from AWS_EBS import AWSEBSExtractor
from AWS_EC2 import AWSEc2Extractor
from AWS_ELB import ELBExtractor

CSV_FOLDER = 'CSV_FOLDER'
YAML_FOLDER = 'YAML_FOLDER'
CSV_FOLDER_2 = 'CSV_METRIC_NAMES'

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
        if not os.path.exists(CSV_FOLDER_2):
            NewFolder3 = CSV_FOLDER_2
            os.mkdir(NewFolder3)


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

    S3Runner = AWSS3Extractor('https://docs.aws.amazon.com/AmazonS3/latest/userguide/metrics-dimensions.html')
    S3Runner.load_page()
    S3Runner.process_content()
    print("Processing Metric: S3")
    S3Runner.generate_yaml()
    S3Runner.generate_csv()
    S3Runner.generate_csv2()
    print("Finished Metric: S3")

    RDSRunner = AWSRDSExtractor('https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/monitoring-cloudwatch.html')
    RDSRunner.load_page()
    RDSRunner.process_content()
    print("Processing Metric: RDS")
    RDSRunner.generate_yaml()
    RDSRunner.generate_csv()
    RDSRunner.generate_csv2()
    print("Finished Metric: RDS")

    EBSRunner = AWSEBSExtractor('https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using_cloudwatch_ebs.html')
    EBSRunner.load_page()
    EBSRunner.process_content()
    print('Processing Metric: EBS')
    EBSRunner.generate_yaml()
    EBSRunner.generate_csv()
    EBSRunner.generate_csv2()
    print("Finished Metric: EBS")

    EC2Runner = AWSEc2Extractor('https://docs.aws.amazon.com/AmazonS3/latest/userguide/metrics-dimensions.html')
    EC2Runner.load_page()
    EC2Runner.process_content()
    print('Processing Metric: EC2')
    EC2Runner.generate_yaml()
    EC2Runner.generate_csv()
    EC2Runner.generate_csv2()
    print("Finished Metric: EC2")

    ELBRunner = ELBExtractor('https://docs.aws.amazon.com/elasticloadbalancing/latest/classic/elb-cloudwatch-metrics.html')
    ELBRunner.load_page()
    ELBRunner.process_content()
    print('Processing Metric: ELB')
    ELBRunner.generate_yaml()
    ELBRunner.generate_csv()
    ELBRunner.generate_csv2()
    print("Finished Metric: ELB")

    print("Task Completed Successfully")



