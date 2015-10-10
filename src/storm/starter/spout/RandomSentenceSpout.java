/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.starter.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.Random;

import storm.starter.util.ReadWriteGod;

public class RandomSentenceSpout extends BaseRichSpout {
  SpoutOutputCollector _collector;
  Random _rand;
  private PrintWriter pw;
  boolean pwEnable;
  String path = "/home/hduser/storm-log/WordCountOutput";
  
  public RandomSentenceSpout(boolean pwEnable){
	  this.pwEnable = pwEnable;
  }
  
  public RandomSentenceSpout(){
	  this.pwEnable = false;
  }

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _collector = collector;
    _rand = new Random();
    if (pwEnable){
        try {
        	String completeFileName = path+
        			"-Spout-"+
            		String.valueOf(context.getThisTaskId())+
            		".txt";
    		pw = ReadWriteGod.writer(completeFileName, true);
    		pw.write("Start Time: " + 
    				String.valueOf(System.currentTimeMillis()) + 
    				"\n" );
    		
    	} catch (IOException e) {
    		// TODO Auto-generated catch block
    		e.printStackTrace();
    	}
    }
  }
  
  public void close(){
	  if (pwEnable){
		  pw.close();
	  }
  }

  @Override
  public void nextTuple() {
    //Commented by Joseph on Oct 2, 2015
	//To control the outgoing data rate, we intensionally add 100ms delay here.
	//The actual flow rate should be controled by the throghput allocation decision.
	Utils.sleep(100);
    String[] sentences = new String[]{ 
    		"Hong Kong, traditionally Hongkong officially known as Hong Kong Special Administrative Region of the People's Republic of China is an autonomous territory on the southern coast of China at the Pearl River Estuary and the South China Sea.",
    		"In the late 1970s, Hong Kong became a major entrept in Asia-Pacific. The territory has developed into a major global trade hub and financial centre,", 
    		"and is regarded as a world city. The 45th-largest economy in the world. Hong Kong ranks top 10 in GDP (PPP) per capita, but also has the most severe income inequality among advanced economies",
    		"Hong Kong is one of the three most important financial centres alongside New York and London. The territory has a high Human Development Index and is ranked highly in the Global Competitiveness Report.",
    		"It has been named the freest market economy by the Heritage Foundation Index of Economic Freedom. The service economy, characterised by low taxation and free trade, has been regarded as one of the world's most laissez-faire economic policies,",
    		"and the currency, the Hong Kong dollar, is the 13th most traded currency in the world. Hong Kong is a member of APEC, ADB, IMF, BIS, WTO, FIFA,",
    		"and International Olympic Committee, as Hong Kong Basic Law authorizes the territory to develop relations with foreign states on its own in appropriate fields,",
    		"including the economic, trade, financial and monetary, shipping, communications, tourism, cultural and sports fields. Hong Kong has carried many nicknames: the most famous among those is the Pearl of the Orient",
    		"which reflected the impressive night-view of the city's light decorations on the skyscrapers along both sides of the Victoria Harbour.",
    		"The territory is also known as Asia's World City. Archaeological studies support human presence in the Chek Lap Kok area (now Hong Kong International Airport) from 35,000 to 39,000 years ago and on Sai Kung Peninsula from 6,000 years ago.",
    		"The area of Hong Kong was consolidated under the kingdom of Nanyue (Southern Viet), founded by general Zhao Tuo in 204 BC after the collapse of the short-lived Qin dynasty.",
    		"When the kingdom of Nanyue was conquered by the Han Dynasty in 111 BC, Hong Kong was assigned to the Jiaozhi commandery. Archaeological evidence indicates that the population increased and early salt production flourished in this time period.",
    		"Lei Cheng Uk Han Tomb on the Kowloon Peninsula is believed to have been built during the Han dynasty. Hong Kong continued to experience modest growth during the first half of the 20th century.",
    		"The University of Hong Kong was established in 1911 as the territory's oldest higher education institute. While there was an exodus of 60,000 residents for fear of a German attack on the British colony during the First World War,",
    		"Hong Kong remained peaceful. Its population increased from 530,000 in 1916 to 725,000 in 1925 and reached 1.6 million by 1941.",
    		"Since Hong Kong's reunification with China, there has been increasing social tension between Hong Kong residents and mainland Chinese due to cultural and linguistic differences, as well as accusations of unruly behaviour and spending habits of mainland Chinese visitors to the territory.",
    		"In 2012 Chief Executive elections saw the Beijing backed candidate Leung Chun-Ying elected with 689 votes from a committee panel of 1,200 selected representatives, and assumed office on 1 July 2012.",
    		"Social conflicts also influenced the mass protests in 2014, primarily caused by the Chinese government's proposal on electoral reform.",
    		"Hong Kong enjoys a high degree of autonomy, as its political and judicial systems operate independently from those of mainland China. In accordance with the Sino-British Joint Declaration, and the underlying principle of one country, two systems, Hong Kong has a high degree of autonomy as a special administrative region in all areas except defence and foreign affairs.",
    		"The guarantees over the territory's autonomy and the individual rights and freedoms are enshrined in the Hong Kong Basic Law, the territory's constitutional document, which outlines the system of governance of the Hong Kong Special Administrative Region, but which is subject to the interpretation of the Standing Committee of the National People's Congress (NPCSC)."};
    String sentence = sentences[_rand.nextInt(sentences.length)];
    pw.write(String.valueOf(System.currentTimeMillis())+": "+
    		String.valueOf(sentence.length())+"\n");
    _collector.emit(new Values(sentence));
  }

  @Override
  public void ack(Object id) {
  }

  @Override
  public void fail(Object id) {
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word"));
  }

}