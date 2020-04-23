package com.community.batch.domain;

import java.time.LocalDateTime;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import com.community.batch.domain.enums.Grade;
import com.community.batch.domain.enums.SocialType;
import com.community.batch.domain.enums.UserStatus;
import com.fasterxml.jackson.annotation.JsonIgnore;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@EqualsAndHashCode(of= {"idx","email"})
@NoArgsConstructor
@Entity
@Table
public class User {

    @Id
    @Column
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long idx;

    @Column
    private String name;

    @Column
    @JsonIgnore
    private String password;

    @Column
    private String email;

    @Column
    private String pincipal;
    
    @Column
    @Enumerated(EnumType.STRING)
	private SocialType socialType;
    
    @Column
    @Enumerated(EnumType.STRING)
    private UserStatus status;
    
    @Column
    @Enumerated(EnumType.STRING)
    private Grade grade;
	
    @Column
    private LocalDateTime createdDate;

    @Column
    private LocalDateTime updatedDate;
    
    @Builder
    public User(String name, String password, String email, String pincipal, SocialType socialType, LocalDateTime createdDate, LocalDateTime updatedDate) {
        this.name = name;
        this.password = password;
        this.email = email;
        this.pincipal = pincipal;
        this.socialType = socialType;
        this.createdDate = createdDate;
        this.updatedDate = updatedDate;
    }
	
    public User setInactive() {
    	status = UserStatus.INACTIVE;
    	return this;
    }
}
