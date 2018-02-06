# Data Loch

Data Loch is an application that runs scheduled scripts to sync latest canvas data dumps (recovered from Canvas Data API) and refreshes Canvas tables housed in the institutional data lake. The application uses AWS services like S3, Elastic Beanstalk and Redshift Spectrum that are assembled together to create a highly available platform for running institutional analytics.

# AWS deployment recipe

## 1. AWS cli & eb cli setup
  ### Install aws cli and eb cli.

  ```
    $ brew install awscli
    or
    $ pip install awscli
  ```

  For more information on installations refer this link:
  http://docs.aws.amazon.com/cli/latest/userguide/installing.html#install-bundle-other-os

  ### Configure aws credentials

  ```
    $ aws configure
    AWS Access Key ID [None]: AKIAIOSFODNN7EXAMPLE
    AWS Secret Access Key [None]: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
    Default region name [None]: region name
    Default output format [None]: ENTER
  ```

  ### Install eb cli
  The Elastic Beanstalk Command Line Interface (EB CLI) is a command line client that you can use to create, configure, and manage Elastic Beanstalk environments. The EB CLI is developed in Python and requires Python version 2.7, version 3.4, or newer.

  ```
    $ brew install python
    $ pip install --upgrade pip
    $ pip install --upgrade --user awsebcli
  ```

  eb is installed to the Python bin directory; add it to your path.

  ```
    $ export PATH=~/Library/Python/2.7/bin:$PATH
  ```

  For more details on install eb refer:
  http://docs.aws.amazon.com/elasticbeanstalk/latest/dg/eb-cli3-install.html

## 2. Pre-deployment set up

  ### Steps
  1. Create dedicated S3 buckets.
  2. Enable transfer acceleration.
  3. Enable encryption using KMS keys.
  4. Set up Lifecycle rules to retain only last 15 days of data in the the bucket/canvas-data/daily/*
  5. Set up a single node Redshift cluster with firewalls
  6. Create a Redshift Spectrum role and provide necessary permissions to S3 bucket

  For more details on Redshift Spectrum set up refer the link below
  https://docs.aws.amazon.com/redshift/latest/dg/c-getting-started-using-spectrum.html

# Elastic Beanstalk setup

## Deploying your application to Elastic Beanstalk via eb cli

  1. Open the terminal and clone the data-loch code form ets-berkeley-edu github.

  ```
      $ git clone https://github.com/ets-berkeley-edu/data-loch.git

      (for specific branches use this)  
      $ git clone -b <branch-name> https://github.com/ets-berkeley-edu/data-loch.git
  ```

  2. Navigate to the node data-loch application folder. Run eb init to initialize the beanstalk application. For fresh environments the following prompts will  appear
      - Default Region - This will be the EC2 location for the application. Choose the nearest one
      - AWS Security credentials
      - EB Application Name (Eg: data-loch)
      - Choice of Programming Language/Framework that your application will be using. In our case Node.js

  ```
      $ eb init

      Select a default region
      1) us-east-1 : US East (N. Virginia)
      2) us-west-1 : US West (N. California)
      3) us-west-2 : US West (Oregon)
      4) eu-west-1 : EU (Ireland)
      5) eu-central-1 : EU (Frankfurt)
      6) ap-south-1 : Asia Pacific (Mumbai)
      7) ap-southeast-1 : Asia Pacific (Singapore)
      8) ap-southeast-2 : Asia Pacific (Sydney)
      9) ap-northeast-1 : Asia Pacific (Tokyo)
      10) ap-northeast-2 : Asia Pacific (Seoul)
      11) sa-east-1 : South America (Sao Paulo)
      12) cn-north-1 : China (Beijing)
      13) us-east-2 : US East (Ohio)
      14) ca-central-1 : Canada (Central)
      15) eu-west-2 : EU (London)
      (default is 3): 3

      Select an application to use
      1) lrs-sqs-poller
      2) canvas-data-processor
      3) boac
      4) lrs-privacy-dashboard
      5) cloud-lrs
      6) [ Create new Application ]
      (default is 5): 6

      Note: Elastic Beanstalk now supports AWS CodeCommit; a fully-managed source control service. To learn more, see Docs: https://aws.amazon.com/codecommit/
      Do you wish to continue with CodeCommit? (y/N) (default is n): n
  ```

  3. List all the eb environments for the application. If the environment is already deployed, then default environment is highlighted with an * symbol. To switch to a different environment use 'eb use' command.

  ```
      $ eb list
      *data-loch-master-dev
      data-loch-worker-dev

      $ eb use data-loch-worker-dev
      $ eb list
      data-loch-master-dev
      *data-loch-worker-dev
  ```

  4. If there are prior deployments to the application then there might be some default configurations associated with elastic beanstalk. This can be confirmed by inspecting the contents of .elasticbeanstalk/config.yml. If there are platform related changes required then use the following

  ```
      $ eb platform list (Gives a list AWS platforms available)
      $ eb platform select

      It appears you are using Node.js. Is this correct?
      (Y/n): Y
  ```

  5. Use eb create to create new environments like dev, prod, qa etc. Be sure to specify EB_ENVIRONMENT variable with the environment name.
    The environment options specified here take precedence. If nothing is specified it takes default values to set up an environment. These can’t be overridden by .ebextensions ami configuration
    Options used
      -  --instance_type - t2.small/micro (instance size).
      -  --elb-type      - By default it seems to pick classic load balancer if not explicitly specified. So, set this to application.
      -  --envvars       - comma separated list of environment variables. No spaces

    More eb create options here
    http://docs.aws.amazon.com/elasticbeanstalk/latest/dg/eb3-create.html


  ```
      $ eb create <environment-name> --elb-type application --instance_type <instance_type> --envvars EB_ENVIRONMENT=<environment-name>

      Eg:
      $ eb create data-loch-master-dev --elb-type application --instance_type t2.micro --envvars EB_ENVIRONMENT=data-loch-master-dev
  ```

  6. The .ebextension folder contains all the custom configurations required to run the data-loch application on the environment.

  7. This should deploy the data-loch application to Elastic Beanstalk. Check the status using the following command

  ```
      $ eb status
      Environment details for: data-loch-master-dev
        Application name: data-loch
        Region: us-west-2
        Deployed Version: app-171211_150149
        Environment ID: e-xxxxxxxxxxx
        Platform: arn:aws:elasticbeanstalk:xxxxxxx::platform/Node.js running on 64bit Amazon Linux/4.4.0
        Tier: WebServer-Standard
        CNAME: xxxxxx.xxxxxx.xxxxxx.elasticbeanstalk.com
        Updated: 2017-12-14 01:39:31.513000+00:00
        Status: Ready
        Health: Green
      Alert: An update to the EB CLI is available. Run "pip install --upgrade awsebcli" to get the latest version.
  ```

  8. After successful deployment start using Codepipeline for subsequent deployments.

  9. To terminate the environments use eb terminate. Be sure to use environment name in the command to avoid goof ups.

  ```
      $ eb use data-loch-master-qa
      $ eb terminate data-loch-master-qa
  ```

  For the full list of EB CLI commands:
  https://docs.aws.amazon.com/elasticbeanstalk/latest/dg/eb3-cmd-commands.html
