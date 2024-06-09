DROP TABLE IF EXISTS Customers CASCADE;
DROP TABLE IF EXISTS Products CASCADE;
DROP TABLE IF EXISTS Categories CASCADE;
DROP TABLE IF EXISTS CartItems CASCADE;
DROP TABLE IF EXISTS Suppliers CASCADE;
DROP TABLE IF EXISTS Orders CASCADE;
DROP TABLE IF EXISTS OrderItems CASCADE;
DROP TABLE IF EXISTS Carts CASCADE;
-- Customers table
CREATE TABLE Customers(
    CustomerID SERIAL PRIMARY KEY,
    FirstName VARCHAR(255),
    LastName VARCHAR(255),
    Email VARCHAR(255),
    Password VARCHAR(255),
    Phone VARCHAR(255),
    Address VARCHAR(255),
    RegistrationDate TIMESTAMP
);
alter table Customers add constraint email_unique unique(Email);

-- Categories table
CREATE TABLE Categories(
    CategoryID SERIAL PRIMARY KEY,
    CategoryName TEXT
);
alter table Categories add constraint CategoryName_unique unique(CategoryName);

-- Suppliers table
CREATE TABLE Suppliers(
    SupplierID SERIAL PRIMARY KEY,
    SupplierName VARCHAR(255),
    ContactEmail VARCHAR(255),
    SupplierPhone VARCHAR(255),
    SupplierAddress VARCHAR(255)
);
alter table Suppliers add constraint ContactEmail_unique unique(ContactEmail);

-- Products table
CREATE TABLE Products(
    ProductID SERIAL PRIMARY KEY,
    ProductName VARCHAR(255),
    Price DECIMAL(10, 2),
    StockQuantity BIGINT,
	ArticleNumber TEXT,
    CategoryID BIGINT,
    SupplierID BIGINT,
	FOREIGN KEY (CategoryID) REFERENCES Categories(CategoryID),
    FOREIGN KEY (SupplierID) REFERENCES Suppliers(SupplierID)
);
alter table Products add constraint ArticleNumber_unique unique(ArticleNumber);

-- Carts table
CREATE TABLE Carts(
    CartID SERIAL PRIMARY KEY,
    CustomerID BIGINT,
    CreationDate TIMESTAMP,
	CartNumber TEXT unique,
	FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID)
);

-- CartItems table
CREATE TABLE CartItems(
    CartItemID SERIAL PRIMARY KEY,
    CartID BIGINT,
    ProductID BIGINT,
    Quantity BIGINT,
	CartNumber TEXT,
	FOREIGN KEY (CartID) REFERENCES Carts(CartID),
    FOREIGN KEY (ProductID) REFERENCES Products(ProductID)
);

-- Orders table
CREATE TABLE Orders(
    OrderID SERIAL PRIMARY KEY,
    OrderDate TIMESTAMP,
    CustomerID BIGINT,
    TotalAmount DECIMAL(10, 2),
    Status VARCHAR(255),
	OrderNumber TEXT,
	FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID)
);
alter table Orders add constraint OrderNumber_unique unique(OrderNumber);
-- OrderItems table
CREATE TABLE OrderItems(
    OrderItemID SERIAL PRIMARY KEY,
    OrderID BIGINT,
    ProductID BIGINT,
    Quantity BIGINT, 
	OrderNumber TEXT,
	FOREIGN KEY (OrderID) REFERENCES Orders(OrderID),
    FOREIGN KEY (ProductID) REFERENCES Products(ProductID)
);
--INDEX
CREATE UNIQUE INDEX idx_order_items ON OrderItems (ProductID, OrderNumber);
CREATE UNIQUE INDEX idx_cart_items ON CartItems (ProductID, CartNumber);
--Customer Information:
SELECT * FROM Customers WHERE CustomerID = 10;

--Product Listing:
SELECT p.ProductID, p.ArticleNumber, p.ProductName, p.Price, p.StockQuantity, c.CategoryName, s.SupplierName
FROM Products p
JOIN Categories c ON p.CategoryID = c.CategoryID
JOIN Suppliers s ON p.SupplierID = s.SupplierID;

--Cart Details:
SELECT ci.CartItemID, ci.Quantity, p.Price, (ci.Quantity * p.Price) as TotalPrice
FROM CartItems ci
JOIN Products p ON ci.ProductID = p.ProductID
WHERE ci.CartID = 4;

--Order Summary:
SELECT o.OrderID, o.OrderDate, o.TotalAmount, o.Status, c.FirstName, c.LastName
FROM Orders o
JOIN Customers c ON o.CustomerID = c.CustomerID
WHERE o.OrderID = 11;


--Functions and Procedures for main actions

--Add Product to Cart:
CREATE OR REPLACE FUNCTION AddProductToCart(cartID BIGINT, productID BIGINT, quantity BIGINT) RETURNS VOID AS $$
BEGIN
    INSERT INTO CartItems (CartID, ProductID, Quantity)
    VALUES (cartID, productID, quantity)
    ON CONFLICT (CartID, ProductID) DO UPDATE SET Quantity = CartItems.Quantity + EXCLUDED.Quantity;
END;
$$ LANGUAGE plpgsql;

--Place Order:
CREATE OR REPLACE FUNCTION PlaceOrder(customerID BIGINT) RETURNS BIGINT AS $$
DECLARE
    newOrderID BIGINT;
    totalAmount DECIMAL(10, 2);
BEGIN
    INSERT INTO Orders (CustomerID, TotalAmount, Status) VALUES (customerID, 0, 'Pending') RETURNING OrderID INTO newOrderID;
    INSERT INTO OrderItems (OrderID, ProductID, Quantity, UnitPrice)
    SELECT newOrderID, ci.ProductID, ci.Quantity, p.Price
    FROM CartItems ci
    JOIN Products p ON ci.ProductID = p.ProductID
    WHERE ci.CartID = (SELECT CartID FROM Carts WHERE CustomerID = customerID);
    
    UPDATE Orders
    SET TotalAmount = (SELECT SUM(oi.Quantity * oi.UnitPrice) FROM OrderItems oi WHERE oi.OrderID = newOrderID)
    WHERE OrderID = newOrderID;

    DELETE FROM CartItems WHERE CartID = (SELECT CartID FROM Carts WHERE CustomerID = customerID);

    RETURN newOrderID;
END;
$$ LANGUAGE plpgsql;

--Triggers

--Update Stock Quantity on Order:
CREATE OR REPLACE FUNCTION UpdateStockQuantity() RETURNS TRIGGER AS $$
BEGIN
    UPDATE Products
    SET StockQuantity = StockQuantity - NEW.Quantity
    WHERE ProductID = NEW.ProductID;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_update_stock_quantity
AFTER INSERT ON OrderItems
FOR EACH ROW
EXECUTE FUNCTION UpdateStockQuantity();

