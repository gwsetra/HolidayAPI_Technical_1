CREATE TABLE country_subdivisions (
    id SERIAL PRIMARY KEY,                  -- Unique identifier for each row
    code VARCHAR(2) NOT NULL,               -- Country code (e.g., 'GB', 'NL')
    name VARCHAR(50) NOT NULL,              -- Country or region name
    subdivisions_code VARCHAR(10) NOT NULL,-- Subdivision code (e.g., 'GB-EAW', 'NL-DR')
    subdivisions_name VARCHAR(50) NOT NULL-- Subdivision name (e.g., 'England and Wales', 'Drenthe')
);