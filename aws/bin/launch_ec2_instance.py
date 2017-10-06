#!/usr/bin/env python
#######################################################################################################################
#                                                                                                                     #
# THIS SCRIPT IS TO PROVISION NEW EC2 INSTANCE, TAG AND CREATE ROUTE53 ENTRY FOR THEM TO REGISTER TO THE SPECIFIC     #
# DOMAIN.                                                                                                             #
# VERSION 1.0                                                                                                         #
# USAGE:                                                                                                              #
#       launch_ec2_instance.py                                                                                     #
#                                                                                                                     #
#######################################################################################################################
import boto3
import time
from pprint import pprint


class aws_api(object):
    """
    This is to create the multiple aws instances on specified subnets
    """
    def __init__(self, subnet_id, vpc_id=None):
        
        AWS_ACCESS_KEY_ID = '<Access Key>'
        AWS_SECRET_ACCESS_KEY = '<secret key>'
        self.subnet_id = subnet_id
        self.vpc_id = vpc_id
        self.ec2_instance = boto3.resource('ec2')
        self.ec2_client = boto3.client('ec2')
        self.ec2 = boto3.resource('ec2',aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name='us-west-2')
        self.subnet = self.ec2_instance.Subnet(self.subnet_id)

    def create_instance(self, image_id, min_count, max_count, key_name, security_group_id, instance_type, availability_zone, subnet):
        """
        Create aws instance based on the specific input parameters
        :return:
        """
        if min_count is None:
            min_count = 1

        if not isinstance(min_count, int):
            raise ValueError("Missing min_count value")

        if not image_id:
            raise ValueError("Missing image_id value")

        instance_id = self.subnet.create_instances(ImageId=image_id, MinCount=min_count, MaxCount=max_count, KeyName=key_name,
                                           SecurityGroupIds=[security_group_id], InstanceType=instance_type,
                                           SubnetId=subnet, Placement={"AvailabilityZone": availability_zone})

        pprint(instance_id)
        return(instance_id)
        

    def create_instance_tag(self, pure_instance_id=None, ec2_full_name = None, deployment = None, project_name = None):
        """
        Create a tag for the specified ec2 instances
        :param self:
        :param pure_instance_id:
        :return:
        """
        if not pure_instance_id:
            raise ValueError("Missing pure instance id")

        if ec2_full_name is None:
            ec2_full_name = "new VM"

        if deployment is None:
            deployment = 'Development'

        if project_name is None:
            project_name = 'Aeris'

        if pure_instance_id is None:
            raise ValueError("Missing pure instance id to process the tagging")

        response = self.ec2_client.create_tags(Resources=[pure_instance_id], Tags=[{'Key': 'Name', 'Value': ec2_full_name}, {'Key': 'Deployment', 'Value': deployment}, {'Key': 'Project', 'Value': project_name}]) 
        pprint (response)
        

    def get_instance_ip_from_id(self, instance_id=None):
        """
        To get the instance IP address from Instance ID
        """
        instance = self.ec2.Instance(instance_id)
        if instance:
            return instance.private_ip_address
        else:
            print("No instance exists with the instance_id : %s " % instance_id)


class AwsRoute53Api(object):
    """
    This is to create the multiple aws instances entry into route53 DNS for setting up the FQDN
    """
    def __init__(self, hosted_zone_id=None):
        """
        initialize the class object for the route 53 input parameters
        :param subnet_id:
        :param vpc_id:
        """
        self.hosted_zone_id = hosted_zone_id
        self.client = boto3.client('route53')


    def create_dns_record_set(self, name, action, ttl, type, ip_address, hosted_zone_id):
        """
        Create route53 recordset for the new domain association
        :param name:
        :param action:
        :param ttl:
        :param type:
        :param ip_address: 
        :param hosted_zone_id:
        :return:
        """
        response = self.client.change_resource_record_sets(
            HostedZoneId=hosted_zone_id,
            ChangeBatch={
                'Comment': 'python api testing hosted zone recordset creation',
                'Changes': [
                    {
                        'Action': 'CREATE',
                        'ResourceRecordSet': {
                            'Name': name,
                            'Type': 'A',
                            'TTL': 300,
                            'ResourceRecords': [
                                {
                                    'Value': ip_address
                                },
                            ],

                        }
                    },
                ]
            }
        )


# Main program execution
def main():

    # Physical directory structure path for the input configuration file to be used to provision the ec2 instance and configure the entries into route53  
    config_file = '/opt/devops/python/conf/instance_input.conf'
    
    # Instance ID for each ec2 instances
    result_instance_id_file = '/opt/devops/python/result/result_instance_id.txt'
    
    # Instance IP address identified from the instanceID in realtime using boto3 to register into route53
    result_instance_ip_address_file = '/opt/devops/python/result/result_instance_ip_address.txt'

    f = open(config_file)
    next(f)
    for line in f:
        print("Config file that is being used is %s" % config_file)
        
        #breaking down the parameters read from the config file to populate the input values from the parameter list
        parameterList = line.split(',')
        ImageId                 = parameterList[0]
        KeyName                 = parameterList[1]
        SecurityGroupId         = parameterList[2]
        InstanceType            = parameterList[3]
        AvailabilityZone1       = parameterList[4]
        Subnet1                 = parameterList[5]
        AvailabilityZone2       = parameterList[6]
        Subnet2                 = parameterList[7]
        EC2Name                 = parameterList[8]
        StartNodeNo             = parameterList[9]
        EndNodeNo               = parameterList[10]
        DomainName              = parameterList[11]
        Deployment              = parameterList[12]
        ProjectName             = parameterList[13]
        HostedZoneId            = parameterList[14]
  
        print("******************* All Paramters Value *********************")
        print("ImageId           = "    + ImageId)
        print("KeyName           = "    + KeyName)
        print("SecurityGroupId   = "    + SecurityGroupId)
        print("InstanceType      = "    + InstanceType)
        print("AvailabilityZone1 = "    + AvailabilityZone1)
        print("Subnet1           = "    + Subnet1)
        print("AvailabilityZone2 = "    + AvailabilityZone2)
        print("Subnet2           = "    + Subnet2)
        print("EC2Name           = "    + EC2Name)
        print("StartNodeNo       = "    + StartNodeNo)
        print("EndNodeNo         = "    + EndNodeNo)
        print("DomainName        = "    + DomainName)
        print("Deployment        = "    + Deployment)
        print("ProjectName       = "    + ProjectName)
        print("HostedZoneId      = "    + HostedZoneId)

    f.close()
    f1 = open(result_instance_id_file,'a')
    f2 = open(result_instance_ip_address_file,'a')

    index = int(StartNodeNo)
    while index <= int(EndNodeNo):
        EC2FullName = EC2Name + str(index) + "." + DomainName
        if index % 2 == 0:
            AvailabilityZone = AvailabilityZone2
            Subnet           = Subnet2
        else: 
            AvailabilityZone = AvailabilityZone1
            Subnet           = Subnet1
        print("EC2FullName = "        + EC2FullName + " : AvailabilityZone = " + AvailabilityZone + " :Subnet = " + Subnet)

        # aws instance initialization
        aws = aws_api(Subnet)
        instance_id=aws.create_instance(ImageId, 1, 1,  KeyName, SecurityGroupId, InstanceType, AvailabilityZone, Subnet)
        time.sleep(5)

        # Attach the tags to ec2 instance
        TempInstanceId = str(instance_id)
        PureInstanceId = TempInstanceId[18:28]
        print(PureInstanceId)
        aws.create_instance_tag(PureInstanceId, EC2FullName, Deployment, ProjectName)
        f1.write(PureInstanceId + '\n')

        # Initialize Route53 entries
        ec2 = boto3.resource('ec2')
        CreatedInstance = ec2.Instance(PureInstanceId)
        CreatedInstanceIpAddress = CreatedInstance.private_ip_address
        
        action = "CREATE"
        ttl = 300
        type_ = 'A'

        route53 = AwsRoute53Api(HostedZoneId)
        route53.create_dns_record_set(EC2FullName, action, ttl, type_, CreatedInstanceIpAddress, HostedZoneId)
        f2.write(CreatedInstanceIpAddress + '\n')
        index = index + 1

    f1.close()
    f2.close()


if __name__ == "__main__":
    main()
