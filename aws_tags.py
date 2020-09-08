import boto3
from tabulate import tabulate
import sys
import os
from dotenv import load_dotenv
import csv
import time

load_dotenv()

def tests():
    sess = get_session('non_prod')
    ec2_list = ec2s(sess, 'us-east-1')
    return ec2_list

def get_session(environment):
    """
    >>> sess = session('non_prod')
    >>> assert isinstance(sess, boto3.session.Session)

    Returns a aws boto3 session according to  
    the environment requested.

    :environment: May be "prod" or "non_prod".

    Return: a aws session.

    Raise: a Key error when there is a missing 
    environment variable.    
    """
    try:
        if environment == 'non_prod':
            session = boto3.Session(
            aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID_NON_PROD'],
            aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY_NON_PROD'])

        if environment == 'prod':
            session = boto3.Session(
            aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY'])
    except KeyError as ex:
        raise Exception('''Missing Enviroment variables AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, 
            AWS_ACCESS_KEY_ID_NON_PROD or AWS_SECRET_ACCESS_KEY_NON_PROD''')
    return session

def ec2s(session, region_name):
    """
    >>> sess = session('non_prod')
    >>> ec2_list = ec2s(sess, 'us-east-1')
    >>> assert isinstance(ec2_list, list)
        
    Return the list of ec2 instances found on a region.

    :session: a boto3 session.
    :region_name: the aws region name being inquired

    Return: a list of aws.boto3 ec2 instances that were 
    found on the region passed.

    """
    ec2 = session.resource('ec2', region_name)
    instances = ec2.instances.all()
    return instances

def tag_values(instances: list):
    tag_values = []
    """
    get all tag values from the instances passed as parameter.

    """
    # adding the tags
    for count, inst in enumerate(instances):
        tags = {tag['Key']: tag['Value'] for tag in inst.tags}
        tags['count'] = count + 1
        tags['instance_id'] = inst.instance_id
        tags['instance_type'] = inst.instance_type
        tags['launch_time'] = inst.launch_time
        tags['state'] = inst.state.get('Name'),
        tags['availability_zone'] = inst.placement.get('AvailabilityZone')
        tags['private_ip_address'] = inst.private_ip_address
        tags['public_ip_address'] = inst.public_ip_address
        tags['instance_type'] = inst.instance_type
        tags['launch_time'] = inst.launch_time

        tag_values.append(tags)
    #print(tag_values)
    return tag_values

def table(tags: list, *tag_names: list):
    """
    convert only the tags passed as a parameter into a list of lists
    from a bigger dictionary
    """
    table = [[str(tag.get(t, '___')) for t in tag_name] for tag in tags for tag_name in tag_names]
    return table

def tag_list(env: str):
    """
    Return a list of distinct tags for an specific environment.
    """
    ec2_list = ec2s(get_session(env), 'us-east-1')
    tags = []
    for instance in ec2_list:
        tags.extend([tag['Key'] for tag in instance.tags])
    # add the other info as tags
    tags.append('instance_id')
    tags.append('instance_type')
    tags.append('launch_time')
    tags.append('state')
    tags.append('availability_zone')
    tags.append('private_ip_address')
    tags.append('public_ip_address')
    tags.append('instance_type')
    tags.append('launch_time')
    stags = list(set(tags))
    stags.sort()
    return stags

def get_csv(field_names, data, env):
    """
    Saves a csv file with the values for the field names
    passed as parameter.

    Args:
        :data: A table object with all tag values.
    """
    # reduce the data to just the fields on field_names
    csv_file =f'{env}_EC2_Tags.csv'
    try:
        with open(csv_file, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=field_names)
            writer.writeheader()
            for dic_item in data:
                fdata = {key: value for key, value in dic_item.items() if key in field_names}
                writer.writerow(fdata)
    except IOError:
        print("I/O error")
    

def get_tag_values(args, env, output):
    """
    Return the tag values on stdout or on a csv file.

    Args:
        :args: List of tag names.
        :env: Environment name: prod or non-prod
        :output: stdout or csv
    """
    field_names = [field_name for field_name in args]
    ec2_list = ec2s(get_session(env), 'us-east-1')
    dic_values = tag_values(ec2_list)
    # TODO: sort here

    data = table(dic_values, field_names)
    if output == 'stdout':
        print(tabulate(data, headers=field_names, tablefmt='orgtbl'))
    else:
        get_csv(field_names, dic_values, env) 
        
def main():
    start = time.time()
    
    if len(sys.argv) < 3:
        print("""
        List EC2 tag values for all instances of an environment.

        Missing parameters:
        Minimum parameters 1 and 2 are expected:

        1 - Environment being requested:
            -prod or -non_prod

        2 - Type of request:
            -tags = Return a list of possible tags
            -values = Return the values of the tags
   
        3 - Output:
            if not supplied, returns the data to the screen.
            -csv = Generates a file named <prod or non_prod>_EC2_Tags.csv with the requested data.

        If request type is -values, the tag names should follow separated by a space.

        Example:
            aws_tags -prod -values count Name instance_id instance_type blc availability_zone private_ip_address Company itemid owner costcenter

        """)
        sys.exit(0)

    args = sys.argv
    args.pop(0) # remove the file name

    output = 'stdout'
    env = 'prod'
    
    if '-prod' in args:
        env = 'prod'
        args.remove('-prod')

    if '-non_prod' in args:
        env = 'non_prod'
        args.remove('-non_prod')

    if '-csv' in args:
        output = 'csv'
        args.remove('-csv')

    if '-tags' in args:
        print(tag_list(env))
        sys.exit(0)
    
    if '-values' in args:
        args.remove('-values')
        get_tag_values(args, env, output)
    end = time.time()
    print(f'Operation took {round(end - start)} secs.')


#s3_dev = session.resource('s3', region_name='us-east-1')
#s3_ids_dev = list(s3_dev.instances.all())
#print(dir(s3_dev))

if __name__ == '__main__':
    main()

