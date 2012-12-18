package cwi.commoncrawl;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;

import com.google.common.collect.ImmutableList;
import com.google.common.net.InternetDomainName;

public class Test {
	private static final String PATH_PREFIX = "hdfs://p-head03.alley.sara.nl/data/public/common-crawl/award/testset/";
	public static void main(String [] args){
		
		String test = " ";
		System.out.println(test+"\t");
		System.out.println(test.concat("\t").concat("mahmoud"));
		
		System.out.println("hello" + "_" +"/");
		
		String path="hdfs://p-head03.alley.sara.nl/data/public/common-crawl/award/testset/textdata-00000";
		System.out.println(path);
		System.out.println(PATH_PREFIX);
		System.out.println(StringUtils.remove(path, PATH_PREFIX));
		System.out.println();
		
		String str = "hello hello h xx gh hello \n hello hello h xx hello h xx";
		System.out.println(str);
		System.out.println(StringUtils.countMatches(str, "hello h xx"));
		
		Pattern pattern = Pattern.compile(".*.nl");
		Matcher matcher = pattern.matcher(path);
		if (matcher.find()) {
		   // System.out.println(matcher.group(0)); //prints /{item}/
		} 
		
		try {
			URI uri = new URI("http://en.wikipedia.org/wiki/Main_Page");
			String host=uri.getHost();
			//System.out.println(host +"\t"+StringUtils.trimToNull(uri.getHost()));
			
			String domain = InternetDomainName.from(host).topPrivateDomain().name();
			String givenDomain = "org1";
			if (domain.matches(".*."+givenDomain)){
				System.out.println("succeed");
			}
			String [] parts = domain.split("\\.");
			System.out.println(parts[parts.length-1]);
			
			if(parts[parts.length-1].equalsIgnoreCase("org")){
				System.out.println("yaaaaaaaaaa");
				
			}
			for (String str1:parts){
				System.out.println(str);
				
			}
			ImmutableList<String> uriParts =InternetDomainName.from(host).parts();
			
			for (String part : uriParts){
				System.out.println(part);
			}
			System.out.println(uri+"\t"+host+"\t"+domain);
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		ArrayList<String> copyFromArray = new ArrayList<String>();
		 String[] keyWords = { "gezocht", "gevraagd",
			"vacature", "vacatures", "vakature", "vakatures", "vacaturenummer",
			"referentienummer", "taakomschrijving", "functieomschrijving",
			"functie omschrijving", "doel van de functie", "sollicitatie",
			"sollicitaties", "solliciteren" };
		 
		 for(String keyword : keyWords){
			 copyFromArray.add(keyword);
			
			 
		 }
		 
		String[] fromArraylistToArray = new String[copyFromArray.size()];
		copyFromArray.toArray(fromArraylistToArray);
		 for(String added :fromArraylistToArray){
			 System.out.println(added);
		 }
		 
		 LongWritable v = new LongWritable(2);
		 LongWritable v1 = new LongWritable();
		 v1.set(v.get());
		 System.out.println(v);
		 System.out.println(v1);
		 
		 
		 String [] toCheck = {"nu.nl",
	                "nuzakelijk.nl",
	                "nusport.nl",
	                "nugeld.nl",
	                "nujij.nl",
	                "zie.nl",
	                "nupubliek.nl",
	                "nuwerk.nl",
	                "nufoto.nl",
	                "nulive.nl",
	                "nujournaal.nl",
	                "nuentoen.nl",
	                "nubijlage.nl"};
		 String start = "nu";
		 for (String str2 : toCheck){
		 System.out.println(str2 + "\t" +str2.startsWith(start));
		 
		 }
		 
		 String line = "ac	http://www.viagra.ac/buy-viagra//diabecon.html";
		 String [] arraycomp;
		 arraycomp = line.split("\t");
		 
		 for(String comp : arraycomp){
			 System.out.println(comp);
		 }
		 float y = 1.4f;
		 System.out.println(Math.round(y));
		 float x= 278446016;
		 //float r = 100000/x;
		// System.out.println(r);
		 float z= (100000f / 278446016f) * 181566959f;
		 int r = Math.round(z);
		 System.out.println(z + "\t"+r);
		 
		 ArrayList<Integer> ar = new ArrayList<Integer>();
		 ar.add(3);
		 ar.add(5);
		 ar.add(10);
		 ArrayList<Integer> res = RandomSampler.randomSample(ar, 2);
		 for(int e : res){
			 System.out.println(e);
			 
		 }
		 if(ar.contains(5)){
			 System.out.println("oooooooooooooooooooo");
		 }
	}

}
