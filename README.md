# PyDBC
Database Connectivity by Python<br>

API:<br>
&nbsp;&nbsp;Parameter:<br>
&nbsp;&nbsp;&nbsp;&nbsp;host : Address<br>
&nbsp;&nbsp;&nbsp;&nbsp;user : User Name<br>
&nbsp;&nbsp;&nbsp;&nbsp;password : Password<br>
&nbsp;&nbsp;&nbsp;&nbsp;db : Database Name<br>
  
&nbsp;&nbsp;Function:<br>
&nbsp;&nbsp;&nbsp;&nbsp;__init__ : Initialize<br>
&nbsp;&nbsp;&nbsp;&nbsp;close : Close Connection<br>
&nbsp;&nbsp;&nbsp;&nbsp;execute : Execute SQL<br>
&nbsp;&nbsp;&nbsp;&nbsp;get_all : Fetch All Rows from Table<br>
&nbsp;&nbsp;&nbsp;&nbsp;get_one : Fetch One Rows from Table<br>
&nbsp;&nbsp;&nbsp;&nbsp;save : Insert Data into Table<br>
&nbsp;&nbsp;&nbsp;&nbsp;update : Update Data<br>
&nbsp;&nbsp;&nbsp;&nbsp;delete : Delete Data<br>
&nbsp;&nbsp;&nbsp;&nbsp;query_close : query & close connection
 
<strong>More Details is on Source</strong><br>

How to Start:<br>
1.Driver Install Command : $ pip3 install --upgrade PyMySQL<br>
2.pyDBC = new PyDBC()<br>
3.Some Operation<br>
4.pyDBC.close()  <--- Important<br>
