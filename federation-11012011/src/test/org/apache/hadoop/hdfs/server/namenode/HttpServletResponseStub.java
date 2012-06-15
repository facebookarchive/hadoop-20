package org.apache.hadoop.hdfs.server.namenode;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Locale;

class HttpServletResponseStub implements HttpServletResponse {
  @Override
  public void addCookie(Cookie cookie) {
  }

  @Override
  public boolean containsHeader(String s) {
    return false;
  }

  @Override
  public String encodeURL(String s) {
    return null;
  }

  @Override
  public String encodeRedirectURL(String s) {
    return null;
  }

  @Override
  public String encodeUrl(String s) {
    return null;
  }

  @Override
  public String encodeRedirectUrl(String s) {
    return null;
  }

  @Override
  public void sendError(int i, String s) throws IOException {
  }

  @Override
  public void sendError(int i) throws IOException {
  }

  @Override
  public void sendRedirect(String s) throws IOException {
  }

  @Override
  public void setDateHeader(String s, long l) {
  }

  @Override
  public void addDateHeader(String s, long l) {
  }

  @Override
  public void setHeader(String s, String s1) {
  }

  @Override
  public void addHeader(String s, String s1) {
  }

  @Override
  public void setIntHeader(String s, int i) {
  }

  @Override
  public void addIntHeader(String s, int i) {
  }

  @Override
  public void setStatus(int i) {
  }

  @Override
  public void setStatus(int i, String s) {
  }

  @Override
  public String getCharacterEncoding() {
    return null;
  }

  @Override
  public String getContentType() {
    return null;
  }

  @Override
  public ServletOutputStream getOutputStream() throws IOException {
    return null;
  }

  @Override
  public PrintWriter getWriter() throws IOException {
    return null;
  }

  @Override
  public void setCharacterEncoding(String s) {
  }

  @Override
  public void setContentLength(int i) {
  }

  @Override
  public void setContentType(String s) {
  }

  @Override
  public void setBufferSize(int i) {
  }

  @Override
  public int getBufferSize() {
    return 0;
  }

  @Override
  public void flushBuffer() throws IOException {
  }

  @Override
  public void resetBuffer() {
  }

  @Override
  public boolean isCommitted() {
    return false;
  }

  @Override
  public void reset() {
  }

  @Override
  public void setLocale(Locale locale) {
  }

  @Override
  public Locale getLocale() {
    return null;
  }
}
