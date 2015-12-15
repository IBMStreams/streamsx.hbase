lHBase Toolkit for Bluemix
==============

This fork of the streamsx.hbase project provides allows Streams to write tuples into an HBase servers in the BigInsights service in Bluemix.
The functionality is provided in the **com.ibm.streamsx.hbase.bluemix** toolkit, only available in the `bluemix` branch of this project.

Unlike the [standard streamsx.hbase toolkit](https://github.com/IBMStreams/streamsx.hbase), this toolkit does not have any operators.  Instead, it only includes SPL functions
that allow 
*    writing to
*    reading from, 
*    creating,
*    and deleting HBase Tables.


It includes samples for each of the functions. 

This toolkit has been tested with HBase 0.98.8, Streams 4.0.1, and BigInsights version 4.0, but is expected to work for any later version of HBase, Streams or BigInsights.  




## Getting started
*   Download the [0.1 release](https://github.com/IBMStreams/streamsx.hbase/releases/tag/bluemix-v0.1.latest). 
*   Look in the samples folder at the `HBaseBluemixSample` project for usage examples of the SPL functions. To run the samples, you will need the URL of the server and a username and password to authenticate.
*   See this [article](https://developer.ibm.com/streamsdev/docs/integrating-streams-biginsights-hbase-service-bluemix) for more information on to get started using the toolkit.

## Building the toolkit

Run `ant` at the top level. This will create a built version of the toolkit in a folder called `tmp` in the top level of the project.
## Troubleshooting

If you run into trouble at any point, please enter an issue on GitHub.  

## Contributing
This repository is using the [fork-and-pull model](https://help.github.com/articles/using-pull-requests).  If you'd like to contribute code, fork a copy of the repository, make changes, and when ready, issue a pull request.  For more details, see the wiki in the IBMStreams/toolkits repository.

## Releases
We will make releases after major features have been added.  If you wish to request a release, please open an issue.

