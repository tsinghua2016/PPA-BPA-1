package spark.prediction.util;

public class DoubleUtil {
	public static double doubleAdd(double a,double b)
	{
		return ((int)(a*100)+(int)(b*100))/100.0;
	}
	public static double doubleSub(double a,double b)
	{
		return ((int)(a*100)-(int)(b*100))/100.0;
	}
	public static double doubleDiv(double a,double b)
	{
		return ((int)((a/b)*100+0.5))/100.0;
	}
	public static double getDoubleValue(String msg) throws Exception
	 {
		double result=0;
		if(msg.endsWith(" ms"))
			result=Double.parseDouble(msg.replace(" ms",""));
		else if( msg.endsWith(" s"))
		{
			result=Double.parseDouble(msg.replace(" s",""))*1000;
		}
		else if(msg.endsWith(" min"))
		{
			result=Double.parseDouble(msg.replace(" min",""))*60*1000;
		}
		else {
			System.out.println(msg);
			throw new Exception("Duration time  unknown");
		}
		 return result;
	 }
}
