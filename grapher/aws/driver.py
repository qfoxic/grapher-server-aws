import asyncio
import boto3
import botocore
import concurrent
import itertools
from collections import defaultdict
from functools import partial
from queue import Queue

from grapher.core import driver
from grapher.core.constants import (UNAUTHORIZED, LINKS_WITH_CYCLE)
from grapher.core.errors import GraphException


REGIONS = (
    'us-east-1', 'us-east-2', 'us-west-1','us-west-2', 'ca-central-1', 'eu-west-1',
    'eu-central-1', 'eu-west-2', 'sa-east-1',
    'ap-southeast-1', 'ap-southeast-2', 'ap-northeast-1', 'ap-northeast-2', 'ap-south-1'
)

# Links should contains tuples (ec2, elb) as keys and a Queue as a value.
LINKS = defaultdict(Queue)

EC2_TYPE = 'ec2'
ELB_TYPE = 'elb'
TAGS_TYPE = 'tags'
SG_TYPE = 'sg'
ASG_TYPE = 'autoscaling'
LINK_TYPE = 'link'
REGION_TYPE = 'region'
VPC_TYPE = 'vpc'

EC2_ID_NAME = 'InstanceId'
ELB_ID_NAME = 'LoadBalancerName'
TAGS_ID_NAME = 'Value'
SG_ID_NAME = 'GroupId'
ASG_ID_NAME = 'AutoScalingGroupName'
VPC_ID_NAME = 'VpcId'

ITEM_ID_FIELD = 'id'
ITEM_PID_FIELD = 'pid'
ITEM_TYPE_FIELD = 'type'

# INFO: general format for commands:
#       data types=ec2
#       data types=ec2,elb


def has_cycles(vertices):
    links = set()
    for t in vertices.split(','):
        edge = tuple(sorted(t.split(':')))
        if edge[0] == edge[1]:
            return True
        if edge not in links:
            links.add(edge)
            continue
        else:
            return True
    return False


def get_links_queue(id1, id2):
    return LINKS[tuple([id1, id2])]


def clear_links_queue():
    global LINKS
    LINKS = defaultdict(Queue)


def start_loop(callback):
    executor = concurrent.futures.ThreadPoolExecutor(
        max_workers=len(REGIONS)
    )
    loop = asyncio.get_event_loop()
    return [
        loop.run_in_executor(executor, partial(callback, region))
        for region in REGIONS
    ]


class Fetcher:
    def __init__(self, keys, flt=None):
        self.keys = keys
        self.fltr = eval('lambda i: {}'.format(flt)) if flt else None

        if self.fltr:
            assert isinstance(self.fltr, type(lambda x: x))

    def fetch(self, region):
        from timeit import default_timer as timer
        cur = timer()
        asyncio.set_event_loop(asyncio.new_event_loop())
        loop = asyncio.get_event_loop()
        res = loop.run_until_complete(partial(self._fetch, region)())
        print('Region: {}, type - {}, time {}'.format(region, self.type, timer() - cur))
        return res

    def _format_link(self, id, pid):
        return {ITEM_TYPE_FIELD: LINK_TYPE, ITEM_ID_FIELD: id, ITEM_PID_FIELD: pid}

    async def _format(self, instance):
        data = {
            ITEM_ID_FIELD: instance[self.field_id_name],
            ITEM_TYPE_FIELD: self.type
        }
        data.update(instance)
        return data

    async def _fetch(self, region):
        try:
            session = boto3.session.Session(
                aws_access_key_id=self.keys[0], aws_secret_access_key=self.keys[1]
            )

            response = await self._instances(session, region)
            if self.fltr:
                return [i async for i in self._extract_data(response, region) if self.fltr(i)]
            return [i async for i in self._extract_data(response, region)]
        except botocore.exceptions.ClientError as err:
            print(err)
            raise GraphException(UNAUTHORIZED)


class EC2Fetcher(Fetcher):
    type = EC2_TYPE
    field_id_name = EC2_ID_NAME

    async def _extract_data(self, response, region):
        links_queue = get_links_queue(EC2_TYPE, SG_TYPE)
        sg_links = get_links_queue(SG_TYPE, EC2_TYPE)
        region_links = get_links_queue(REGION_TYPE, EC2_TYPE)
        vpc_links = get_links_queue(VPC_TYPE, EC2_TYPE)

        for rec in response['Reservations']:
            for instance in rec['Instances']:
                region_links.put(self._format_link(
                    instance[EC2_ID_NAME], region)
                )
                # Stopped instances don't have VPC ID.
                if instance.get(VPC_ID_NAME):
                    vpc_links.put(self._format_link(
                        instance[EC2_ID_NAME], instance[VPC_ID_NAME])
                    )
                for i in instance['SecurityGroups']:
                    links_queue.put(self._format_link(i[SG_ID_NAME], instance[EC2_ID_NAME]))
                    sg_links.put(self._format_link(instance[EC2_ID_NAME], i[SG_ID_NAME]))

                yield await self._format(instance)

    async def _instances(self, session, region):
        return session.client(self.type, region_name=region).describe_instances()


class RegionFetcher(Fetcher):
    type = REGION_TYPE

    def fetch(self, region):
        return [{
            ITEM_ID_FIELD: region,
            ITEM_TYPE_FIELD: self.type,
            'Name': region
        }]


class ASGFetcher(Fetcher):
    type = ASG_TYPE
    field_id_name = ASG_ID_NAME

    async def _extract_data(self, response, region):
        links_queue = get_links_queue(EC2_TYPE, ASG_TYPE)

        for rec in response['AutoScalingGroups']:
            for i in rec['Instances']:
                links_queue.put(self._format_link(i[EC2_ID_NAME], rec[ASG_ID_NAME]))

            yield await self._format(rec)

    async def _instances(self, session, region):
        return session.client(self.type, region_name=region).describe_auto_scaling_groups()


class ELBFetcher(Fetcher):
    type = ELB_TYPE
    field_id_name = ELB_ID_NAME

    async def _extract_data(self, response, region):
        elb_queue = get_links_queue(ELB_TYPE, EC2_TYPE)
        ec2_queue = get_links_queue(EC2_TYPE, ELB_TYPE)
        sg_queue = get_links_queue(ELB_TYPE, SG_TYPE)
        elb_sg_queue = get_links_queue(SG_TYPE, ELB_TYPE)
        region_links = get_links_queue(REGION_TYPE, ELB_TYPE)

        for instance in response['LoadBalancerDescriptions']:
            region_links.put(self._format_link(
                instance[ELB_ID_NAME], region)
            )
            # Fill ec2 links.
            for i in instance['Instances']:
                elb_queue.put(self._format_link(i[EC2_ID_NAME], instance[ELB_ID_NAME]))
                ec2_queue.put(self._format_link(instance[ELB_ID_NAME], i[EC2_ID_NAME]))

            # Fill sg links.
            for i in instance['SecurityGroups']:
                sg_queue.put(self._format_link(i, instance[ELB_ID_NAME]))
                elb_sg_queue.put(self._format_link(instance[ELB_ID_NAME], i))

            yield await self._format(instance)

    async def _instances(self, session, region):
        return session.client(self.type, region_name=region).describe_load_balancers()


class TAGSFetcher(Fetcher):
    """This fetcher extracts only links built for EC2 instances."""
    type = TAGS_TYPE

    async def _extract_data(self, response, region):
        result = set()
        for tag in response['Tags']:
            key, value = tag['Key'], tag['Value']
            if key.isalpha():
                queue_type = 'tags-{}'.format(key.lower().strip())
                queue = get_links_queue(EC2_TYPE, queue_type)
                queue.put(self._format_link(tag['ResourceId'], value))
            result.add((key, value))

        for key, val in result:
            yield {ITEM_ID_FIELD: val, ITEM_TYPE_FIELD: self.type, 'Key': key, 'Value': val}

    async def _instances(self, session, region):
        return session.client(EC2_TYPE, region_name=region).describe_tags(
            Filters=[{'Name': 'resource-type', 'Values': ['instance']}]
        )


class SGFetcher(Fetcher):
    """This fetcher extracts only links built for EC2 instances."""
    type = SG_TYPE
    field_id_name = SG_ID_NAME

    async def _extract_data(self, response, region):
        region_links = get_links_queue(REGION_TYPE, SG_TYPE)

        for sg in response['SecurityGroups']:
            region_links.put(self._format_link(
                sg[SG_ID_NAME], region)
            )
            yield await self._format(sg)

    async def _instances(self, session, region):
        return session.client(EC2_TYPE, region_name=region).describe_security_groups()


class VPCFetcher(Fetcher):
    """This fetcher extracts only links built for EC2 instances."""
    type = VPC_TYPE
    field_id_name = VPC_ID_NAME

    async def _extract_data(self, response, region):
        region_links = get_links_queue(REGION_TYPE, VPC_TYPE)

        for vpc in response['Vpcs']:
            region_links.put(self._format_link(
                vpc[VPC_ID_NAME], region)
            )

            yield await self._format(vpc)

    async def _instances(self, session, region):
        return session.client(EC2_TYPE, region_name=region).describe_vpcs()


def ec2(keys, fltr):
    """Returns generator with a task results as soon as completed."""
    return start_loop(EC2Fetcher(keys, fltr).fetch)


def region(keys, fltr):
    """Returns generator with a task results as soon as completed."""
    return start_loop(RegionFetcher(keys, fltr).fetch)


def elb(keys, fltr):
    return start_loop(ELBFetcher(keys, fltr).fetch)


def tags(keys, fltr):
    return start_loop(TAGSFetcher(keys, fltr).fetch)


def sg(keys, fltr):
    return start_loop(SGFetcher(keys, fltr).fetch)


def asg(keys, fltr):
    return start_loop(ASGFetcher(keys, fltr).fetch)


def vpc(keys, fltr):
    return start_loop(VPCFetcher(keys, fltr).fetch)


COLLECTORS = {
    EC2_TYPE: ec2,
    ELB_TYPE: elb,
    TAGS_TYPE: tags,
    SG_TYPE: sg,
    ASG_TYPE: asg,
    REGION_TYPE: region,
    VPC_TYPE: vpc
}


class AWSDriver(driver.AbstractDriver):

    def _links(self, types):
        """
        Display links between different objects.

        :param types: string in a following format: a:b,a:c 
        :return: generator 
        """
        if has_cycles(types):
            raise GraphException(LINKS_WITH_CYCLE)
        aws_types = types.split(',')
        for aws_type in aws_types:
            type1, type2 = aws_type.split(':')
            data = get_links_queue(type1, type2)
            while not data.empty():
                yield data.get()

    def auth(self, key, secret):
        self.keys = key, secret

    def data(self, **kwargs):
        """
        This method should return list of dicts.
        
        :param types: string with a comma separated values: a,b,c 
        :param links: list of comma separated links - ec2:elb, ec2:sg. No cycles are allowed.
        :param kwargs: is a dict with a filters like - ec2="attr1 == 1", elb="attr2 == 2"   
        :return: generator
        """
        types = kwargs['types']
        links = kwargs.get('links')
        filters = {
            key: val for key, val in kwargs.items() if key in COLLECTORS
        }
        aws_types = types.split(',')
        loop = asyncio.get_event_loop()
        futures = [
            COLLECTORS[aws_type](
                self.keys, filters.get(aws_type)) for aws_type in aws_types if aws_type in COLLECTORS
        ]

        clear_links_queue()

        for future in asyncio.as_completed(itertools.chain.from_iterable(futures)):
            type_data_list = loop.run_until_complete(future)
            for data_list in type_data_list:
                yield data_list

        if links:
            yield from self._links(links)

    def info(self):
        yield {
            'driver': 'aws',
            'available_links': ', '.join(['{}:{}'.format(t1, t2) for t1, t2 in LINKS])
        }
