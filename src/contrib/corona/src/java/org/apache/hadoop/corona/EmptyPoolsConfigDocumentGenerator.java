package org.apache.hadoop.corona;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * Used for testing, simply generates an empty document with increasing
 * versions.
 */
public class EmptyPoolsConfigDocumentGenerator implements
    PoolsConfigDocumentGenerator {
  /** Class logger */
  private static final Log LOG =
      LogFactory.getLog(EmptyPoolsConfigDocumentGenerator.class);
  /** Versions of the document that have been created */
  private int documentVersion = 0;

  @Override
  public void initialize(CoronaConf conf) {
  }

  @Override
  public Document generatePoolsDocument() {
    DocumentBuilderFactory documentBuilderFactory =
        DocumentBuilderFactory.newInstance();
    Document document;
    try {
      document =
          documentBuilderFactory.newDocumentBuilder().newDocument();
    } catch (ParserConfigurationException e) {
      throw new IllegalStateException(
          "generatePoolConfig: Failed to create a new document");
    }
    Element root =
        document.createElement(ConfigManager.CONFIGURATION_TAG_NAME);
    document.appendChild(root);
    root.setAttribute("version", Integer.toString(documentVersion));
    LOG.info("generatePoolsDocument: Creating version " + documentVersion);
    ++documentVersion;

    return document;
  }
}
