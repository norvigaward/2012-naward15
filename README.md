naward15
========

1. MapReduce
Main classes:

1.1. CountHostDomainUrls.java

	The main task of the MapReduce job that uses this main class is to count number of URLs for each host domain. This job has two pases map and reduce. The map task 		input key/value pairs are URL/metdata from the metadata files of the common-crawl corpus, the map will get the host domain of the URL and ouput <host_domain, 		count(1)>. The reduce task will have as input <host_domain,{1,1,...,1}> and will outout <host_domain,sum(of input 1's)>.
	

1.2. ArcLookupIndex.java

	The main task is to create a lookup index which contains information about all the URLs in the common-crawl corpus, such as which ARC file contains which URL and  		what is the offset. We can read the ARC file at the specified offset to locate the URL and it's payload. We run this MapReduce job over the common-crawl corpus  	 metadata files. content of the metadata file is key/value pairs; where the key is the URL and the value is the metadata of that URL. The main work is done by the 		map task, and the reduce task is just only to combine map tasks output into one single file.
	ouput: <domainID URL Arcfile offset> 


1.3. SearchTextDataForVacancyMatches.java

	The main task is to find which URL from the common-crawl corpus matches given vacancy keywords. The MapReduce job run over the common-crawl textData files, each    		textData file is a HadoopSequence file, where URL is used as the key and the real text from that URL as value. The output of the MapReduce job tells us which URLs 
	have matched the vacancy keywords filter (at least has two matches) and what are the matched keywords and the frequency for each matched keyword. 
	output: <(domain_ID, URL) (matched_keyword,frequency)>; where (domain_ID, URL) comopse the MapReduce output key and (matched_keyword,frequency) compose the value. 
	Number of outputs for each URL is equal to the number of matches.



1.4. CreateSample.java

	The main task is to produce a sample list of URLs, we will use this list in (5) to extract HTML content. The input for this MapReduce job is a file which contains
	domain IDs and their URLs, for each domain ID we pick number of URLs randomly. 
	output: <domainID,URL>; where domainID is the key and URL is the value. Number of outputs for any domain ID is equal to the number of URLs for that domain ID

1.5. ExtractHtmlFromArc.java

	The main task is to extract HTML contents from a given list of URLs. 
	output: <URL,HTMl>

2. PigLatin:

We use PigLatin for 
a) further processing of some MapReduce jobs and 
b) preparing the input to some MapReduce jobs

2.1. Output from MapReduce(1.3)
	
	We group the result by domain ID and URL, to have a list of matched keywords for every URL
	output: <(domainID,URL),{(matched_keyword1,frequency1), (matched_keyword2,frequency2), ..., (matched_keyword_n,frequency_n)}>

2.2. Input to MapReduce(1.4)

	The input to MapReduce(4) should be list of domain IDs and their URLs, get that by extracting them from 2.1

2.3. Input to MapReduce(1.5)

	To extract the HTML from the given list of URLs, we need to read the ARC files which contain those URLs inorder to extract their HTML. We use PigLatin 
 	to join output of (1.5) and output of (1.2), we get <URL,ArcFile>
