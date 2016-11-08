package udacity.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import udacity.storm.tools.Rankings;
import udacity.storm.tools.RankableObjectWithFields;
import udacity.storm.tools.Rankable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.Map;

/**
 * A bolt that parses the tweet into words
 */
public class NewBolt extends BaseRichBolt
{
  // To output tuples from this bolt to the count bolt
  OutputCollector collector;

  private  Rankings rankableList;
  private String sentence;

  @Override
  public void prepare(
      Map                     map,
      TopologyContext         topologyContext,
      OutputCollector         outputCollector)
  {
    // save the output collector for emitting tuples
    collector = outputCollector;
  }

  @Override
  public void execute(Tuple tuple)
  {
     if (tuple.getSourceComponent().equals("rankings")){
       rankableList = (Rankings) tuple.getValue(0);
     } else {
       sentence = tuple.getString(0);
       if (rankableList!=null){
         for (Rankable r: rankableList.getRankings()){
             String word = r.getObject().toString();

             if (sentence.contains(word))
                collector.emit(new Values(sentence));
           }
       }
     }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer)
  {
    // tell storm the schema of the output tuple for this spout
    // tuple consists of a single column called 'tweet-word'
    declarer.declare(new Fields("top-tweet"));
  }
}
