package com.community.batch.repository;

import java.time.LocalDateTime;
import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.community.batch.domain.User;
import com.community.batch.domain.enums.Grade;
import com.community.batch.domain.enums.UserStatus;

@Repository
public interface UserRepository extends JpaRepository<User, Long>{
	List<User> findByUpdatedDateBeforeAndStatusEquals(LocalDateTime localDateTime,UserStatus status);
	
	List<User> findByCreatedDateBeforeAndStatusEqualsAndGradeEquals(LocalDateTime localDateTime,UserStatus status,Grade grade);
}
