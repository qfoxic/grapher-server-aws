## AWS Driver
#### Introduction
This driver covers some of AWS EC2 assets.
To make everything work you have to authorize with you access and secret keys:
> load aws
> auth key=ACCESS_KEY&&secret=SECRET_KEY

Now, you can start querying for a different assets:
> data types=ec2

That command about will return all ec2 instances across all regions.

If you need to see which instances are located in which regions you can run:
> data types=ec2,region&&links=region:ec2

That command will return two types of data: `ec2` and `region`. And also returns
links between them.

If you wish to filter out stopped ec2 instances, you can run:
> data types=ec2,region&&links=region:ec2&&ec2=i["State"]["Name"] =="running"

Currently supported types: ec2, elb, tags, sg, asg, region, vpc

