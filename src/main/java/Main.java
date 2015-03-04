import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pattern.pmml.PMMLPlanner;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

import java.io.File;
import java.util.*;

/**
 * Created by stendu on 2/22/2015.
 */
public class Main {
    public static void main(String[] args) throws RuntimeException
    {
        //Assign input and output file names
        String inputPath = args[0];
        String outputPath = args[1];
        String pmmlPath = (String) args[2];
        Map<String,String> desc = new HashMap<String, String>();
        desc.put("1","predict");
        //set up the config properties
        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties,Main.class);
        HadoopFlowConnector flowConnector = new HadoopFlowConnector(properties);

        //create source and sink taps
        Tap inputTap = new Hfs(new TextDelimited(true, ","), inputPath);
        Tap outputTap = new Hfs( new TextDelimited( true, "," ), outputPath );

        //connect tap, pipes etc
        FlowDef flowDef = FlowDef.flowDef()
                .setName("predict")
                .addDescriptions(desc)
                .addSource("input", inputTap)
                .addSink("output", outputTap);

        PMMLPlanner pmmlPlanner = new PMMLPlanner()
                .setPMMLInput(new File(pmmlPath))
                .retainOnlyActiveIncomingFields()
                .setDefaultPredictedField(new Fields("predict", Double.class)); // default value if missing from the model

        flowDef.addDescriptions(desc);
        pmmlPlanner.getFlowDescriptor();

        flowDef.addAssemblyPlanner(pmmlPlanner);

        // write a DOT file and run the flow
        Flow classifyFlow = flowConnector.connect( flowDef );
        classifyFlow.writeDOT( "dot/classify.dot" );
        classifyFlow.complete();
    }
}
