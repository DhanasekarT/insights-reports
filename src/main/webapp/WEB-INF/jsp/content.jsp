<%@ taglib prefix="c" uri="http://java.sun.com/jstl/core" %>
<%@ page contentType="application/json;charset=utf8" %>
<%
  String callback = request.getParameter("callback");
 response.setContentType("application/json");
  response.setHeader("Content-Disposition", "inline");
  if (callback != null) {
%><%=callback%>(
<%= request.getAttribute ("content").toString()  %>
);<% } else {
response.getWriter().write(request.getAttribute ("content").toString());
%>
<% } %>
