package com.next.storm.integration;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import org.apache.storm.flux.model.BeanDef;
import org.apache.storm.flux.model.BoltDef;
import org.apache.storm.flux.model.PropertyDef;
import org.apache.storm.flux.model.TopologyDef;
import org.apache.storm.flux.parser.FluxParser;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import backtype.storm.generated.StormTopology;
import backtype.storm.task.IBolt;

public class TestStormTopologyBuilder {

	public static void main(String[] args) throws Exception {
		TestStormTopologyBuilder testStormTopologyBuilder = new TestStormTopologyBuilder();
		testStormTopologyBuilder.buildTopology("test-topology.yaml", "DissectMessage-Bolt");
	}

	public StormTopology buildTopology(String fluxTopologyFileName, String boltName) throws Exception {
		Resource resource = new ClassPathResource(fluxTopologyFileName);
		TopologyDef topologyDef = FluxParser.parseFile(resource.getFile().getAbsolutePath(), false, false, null, false);
		IBolt bolt = buildBolt(topologyDef, boltName);
		return null;
	}

	private Object buildObject(TopologyDef topologyDef, String className, List<PropertyDef> properties)
			throws Exception {
		Class cls = Class.forName(className);
		Object object = cls.newInstance();
		if (properties == null) {
			return object;
		}
		for (PropertyDef onePropertyDef : properties) {
			System.out.println(
					onePropertyDef.getName() + " : " + onePropertyDef.getValue() + " : " + onePropertyDef.getRef());
			if (onePropertyDef.getValue() == null) {
				setProperty(object, onePropertyDef.getName(), buildRef(topologyDef, onePropertyDef.getRef()));
			} else {
				System.out.println(onePropertyDef.getName() + " : " + onePropertyDef.getValue().getClass().getName());
				setProperty(object, onePropertyDef.getName(), onePropertyDef.getValue());
			}
		}
		return object;
	}

	private IBolt buildBolt(TopologyDef topologyDef, String boltName) throws Exception {
		BoltDef boltDef = topologyDef.getBoltDef(boltName);
		String className = boltDef.getClassName();

		return (IBolt) buildObject(topologyDef, className, boltDef.getProperties());
	}

	private Object buildRef(TopologyDef topologyDef, String refName) throws Exception {
		BeanDef beanDef = topologyDef.getComponent(refName);
		String className = beanDef.getClassName();
		return buildObject(topologyDef, className, beanDef.getProperties());
	}

	private void setProperty(Object obj, String fieldName, Object fieldValue)
			throws InvocationTargetException, IllegalAccessException {
		Method[] methods = obj.getClass().getMethods();
		String methodName = "set" + Character.toUpperCase(fieldName.toCharArray()[0]) + fieldName.substring(1);
		System.out.println("Method to lookup : " + methodName);
		for (Method oneMethod : methods) {
			if (oneMethod.getName().equals(methodName)) {
				oneMethod.invoke(obj, fieldValue);
			}
		}
	}

}
