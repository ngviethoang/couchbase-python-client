CREATE TABLE `movies` (
	`id` INT NOT NULL AUTO_INCREMENT,
	`title` varchar(255) NOT NULL,
	`release_year` INT(4),
	PRIMARY KEY (`id`)
);

CREATE TABLE `ratings` (
	`id` INT NOT NULL AUTO_INCREMENT,
	`movie_id` INT NOT NULL,
	`customer_id` INT NOT NULL,
	`rating` INT NOT NULL,
	PRIMARY KEY (`id`)
);

CREATE TABLE `customers` (
	`id` INT NOT NULL AUTO_INCREMENT,
	`name` varchar(255) NOT NULL,
	`address` varchar(255),
	PRIMARY KEY (`id`)
);

ALTER TABLE `ratings` ADD CONSTRAINT `ratings_fk0` FOREIGN KEY (`movie_id`) REFERENCES `movies`(`id`);

ALTER TABLE `ratings` ADD CONSTRAINT `ratings_fk1` FOREIGN KEY (`customer_id`) REFERENCES `customers`(`id`);

