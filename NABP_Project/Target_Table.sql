CREATE TABLE public.state_data (
    id INT IDENTITY(1,1),
    state VARCHAR(255),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
