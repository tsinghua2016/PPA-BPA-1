package spark.prediction;
import static spark.prediction.util.DoubleUtil.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import org.apache.spark.prediction.TaskPrediction;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
public class Main {
	private String  appid="";
	private String url="http://127.0.0.1:7777";
	private int startStageId=0;
	private int endStageId=0;
	private float core=1;
	private LinkedList<Integer> tasksList=new LinkedList<Integer>();

	private static final int TIMEOUT=60*60*1000;
	public static void main(String[] argv)
	{
		new Main().start(argv);
	}
	public void start(String[] argv)
	{

		if(argv.length<4)
		{
			System.out.println("Usage: <url> <appid> <stage_start_id> <stage_end_id> [factor]");
			System.exit(-1);
		}
		if(argv.length==5)
			core=Float.parseFloat(argv[4]);
		try {
			url = argv[0];
			appid = argv[1];
			startStageId = Integer.parseInt(argv[2]);
			endStageId = Integer.parseInt(argv[3]);
 
			if(endStageId < startStageId)throw new Exception("stage_end_id < stage_start_id");
			action();
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		
	}
	
	 @SuppressWarnings("unchecked")
	public void action() throws Exception
	 {
		
		 String head=url+"/history/"+appid+"/stages/";
		 Document doc = Jsoup.connect(head).maxBodySize(Integer.MAX_VALUE).timeout(TIMEOUT).get();
		   String failed = doc.select("#failed").text();
			String appname=doc.select(".navbar-text strong").get(0).text().split(" ")[0];
			Elements tables = doc.select("table");
			int tableindex=tables.size()-1;
			if(!failed.equals(""))
			{
				tableindex=tableindex-1;
			}
			
			Elements trs = tables.get(tableindex).select("tbody tr"); 
			Iterator<Element> iterator = trs.iterator();
			
			HashMap<String,LinkedList<Double>> map=new HashMap<String,LinkedList<Double>>();
			int index=0;
			while(iterator.hasNext())
			{
				
				Element elem = iterator.next();
			
				if(elem.child(1).text().contains("Unknown Stage Name"))
					continue;
			
				int id=Integer.parseInt(elem.child(0).text());
			
				if(id>=startStageId && id<=endStageId)
				{
					index++;
					String stagename=elem.child(1).select(".name-link").text();
					double stageduration = getDoubleValue(elem.child(3).text());
					String tempurl=head+"stage/?id="+id+"&attempt=0";
					Document stage = Jsoup.connect(tempurl).timeout(TIMEOUT).get();
					Elements elements =stage.select("#task-table tbody tr");
					Iterator<Element> iteratort = elements.iterator();
					LinkedList<Double> list=null;
					if(map.containsKey(stagename))
					{
						list=map.get(stagename);
					}
					else
					{
						list=new LinkedList<Double>();
						map.put(stagename, list);
					}
					 while(iteratort.hasNext())
					 {
						Element elemt = iteratort.next();
						double tasktime = getDoubleValue(elemt.child(7).text());
						
						//count
						double taskconsume = (int)(tasktime*100/stageduration*core)/100.0;
						list.add(taskconsume);
						//save all data
						tasksList.add((int)(taskconsume*100));
					 }
					 
				}
				
			}//end 
			System.out.println("stage_quantity => "+index);
			saveTaskAllData(appname+".txt");
			BuildObjFile(appname, map);
			

		
	  }

	private void BuildObjFile(String appname,
			HashMap<String, LinkedList<Double>> map)
			throws FileNotFoundException, IOException {
		File savefile=new File(appname+".obj");
		FileOutputStream fo=new FileOutputStream(savefile);
		
		ObjectOutputStream ow=new ObjectOutputStream(fo);
		Iterator<String> keyiter = map.keySet().iterator();
		HashMap<String,TaskPrediction> data=new HashMap<String,TaskPrediction>();
		while(keyiter.hasNext())
		{
			String key=keyiter.next();
			if(key.contains("start at") || key.contains("submitJob at"))
				continue;
			LinkedList<Double> list = map.get(key);
			double sum=0;
			for(double dt:list)
			{
				sum=doubleAdd(dt,sum);
			}
			int size=list.size();
			double ave=doubleDiv(sum,size);
			sum=0;
			for(double dt:list)
			{
				sum=doubleAdd(Math.pow(doubleSub(dt,ave), 2),sum);
			}
			double sd = ((int)(Math.sqrt(doubleDiv(sum,size))*100))/100.0;
			TaskPrediction tp =new TaskPrediction(sd, ave);
			data.put(key,tp);
		}
  
		ow.writeObject(data);
		ow.close();
		fo.close();
	}
	private void saveTaskAllData(String filename) throws Exception
	{
		if(tasksList!=null && tasksList.size()>0)
		{
			
			FileOutputStream fo=new FileOutputStream(filename);
			Iterator<Integer> iterator = tasksList.iterator();
			while(iterator.hasNext())
			{
				String data=iterator.next()+" ";
				fo.write(data.getBytes());
			}
			fo.close();
		}
	}
	 

	
	
}
