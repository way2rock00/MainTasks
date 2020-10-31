REM ================================================================================
REM  Program:      SLC_ISPINF_FRANCON_RESP_TAB.sql		                                
REM  Author:       Akshay Nayak                                           
REM  Date:         14-Aug-2017                                                      
REM  Purpose:      Purpose of this is to create response object for FranConnect service handshake to work
REM  Change Log:  14-Aug-2017     Akshay Nayak  Created									         
REM  ================================================================================

CREATE OR REPLACE TYPE APPS.SLC_ISPINF_FRANCON_RESP_TAB AS TABLE OF SLC_ISPINF_FRANCON_RESP_OBJ;
/
SHOW ERROR;
EXIT;

