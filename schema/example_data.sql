INSERT INTO PART VALUES (1, 'Part 1', 'Manufacturer 1', 'Brand 1', 'Type 1', 100, 'Ctr 1', 200.00, 'Comment 1');
INSERT INTO SUPPLIER VALUES (1, 'Supplier 1', 'Address 1', 1, '123-456-7890', 1000.00, 'Comment 2');
INSERT INTO PARTSUPP VALUES (1, 1, 50, 500.00, 'Comment 3');
INSERT INTO CUSTOMER VALUES (1, 'Customer 1', 'Address 2', 1, '098-765-4321', 1500.00, 'Segment 1', 'Comment 4');
INSERT INTO ORDERS VALUES (1, 1, 'O', 1000.00, '2023-07-12', 'High', 'Clerk 1', 10, 'Comment 5');
INSERT INTO LINEITEM VALUES (1, 1, 1, 1, 10, 1000.00, 0.10, 0.10, 'R', 'O', '2023-07-12', '2023-07-12', '2023-07-12', 'Ship Instruct 1', 'shmode 1', 'Comment 6');
INSERT INTO NATION VALUES (1, 'Nation 1', 1, 'Comment 7');
INSERT INTO REGION VALUES (1, 'Region 1', 'Comment 8');
