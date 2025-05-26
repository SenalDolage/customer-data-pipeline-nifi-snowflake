MERGE INTO customer c
USING customer_raw cr
    ON c.customer_id = cr.customer_id
WHEN MATCHED AND c.customer_id <> cr.customer_id or
                 c.first_name  <> cr.first_name  or
                 c.last_name   <> cr.last_name   or
                 c.email       <> cr.email       or
                 c.street      <> cr.street      or
                 c.city        <> cr.city        or
                 c.state       <> cr.state       or
                 c.country     <> cr.country then update
    set c.customer_id = cr.customer_id
        ,c.first_name  = cr.first_name 
        ,c.last_name   = cr.last_name  
        ,c.email       = cr.email      
        ,c.street      = cr.street     
        ,c.city        = cr.city       
        ,c.state       = cr.state      
        ,c.country     = cr.country  
        ,update_timestamp = current_timestamp()
WHEN NOT MATCHED THEN INSERT
(c.customer_id,c.first_name,c.last_name,c.email,c.street,c.city,c.state,c.country)
    VALUES (cr.customer_id,cr.first_name,cr.last_name,cr.email,cr.street,cr.city,cr.state,cr.country);


SELECT * FROM customer;
SELECT * FROM customer_raw;

TRUNCATE customer;



CREATE OR REPLACE PROCEDURE pdr_scd_demo()
returns string not null
language javascript
as
    $$
      var cmd = `
                 merge into customer c 
                 using customer_raw cr
                    on  c.customer_id = cr.customer_id
                 when matched and c.customer_id <> cr.customer_id or
                                  c.first_name  <> cr.first_name  or
                                  c.last_name   <> cr.last_name   or
                                  c.email       <> cr.email       or
                                  c.street      <> cr.street      or
                                  c.city        <> cr.city        or
                                  c.state       <> cr.state       or
                                  c.country     <> cr.country then update
                     set c.customer_id = cr.customer_id
                         ,c.first_name  = cr.first_name 
                         ,c.last_name   = cr.last_name  
                         ,c.email       = cr.email      
                         ,c.street      = cr.street     
                         ,c.city        = cr.city       
                         ,c.state       = cr.state      
                         ,c.country     = cr.country  
                         ,update_timestamp = current_timestamp()
                 when not matched then insert
                            (c.customer_id,c.first_name,c.last_name,c.email,c.street,c.city,c.state,c.country)
                     values (cr.customer_id,cr.first_name,cr.last_name,cr.email,cr.street,cr.city,cr.state,cr.country);
      `
      var cmd1 = "truncate table SCD_DEMO.SCD2.customer_raw;"
      var sql = snowflake.createStatement({sqlText: cmd});
      var sql1 = snowflake.createStatement({sqlText: cmd1});
      var result = sql.execute();
      var result1 = sql1.execute();
    return cmd+'\n'+cmd1;
    $$;
call pdr_scd_demo();



    